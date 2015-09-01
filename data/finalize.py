import sys, argparse, threading
from pymongo import MongoClient
from bson import ObjectId
from bson import SON

ORDER_DELTA_MAP_FN = """
function() {
  var key = {
    champion: this.value.championId,
    role: this.value.role,
    lane: this.value.lane,
    _key: this.value.finalBuild.sort().join(",")
  }
  var value = this.value
  value.playrate = value.stats.count;
  value.deltas = [];
  emit(key, value);
}
"""

DELTA_REDUCE_FN = """
function(key, values) {
  var better = function(a, b) {
    if (a.stats.count > b.stats.count) {
      return true;
    } else if (a.stats.count === b.stats.count && 
      (a.stats.wins / a.stats.count) > (b.stats.wins / b.stats.count)) {
      return true;
    } else {
      return false;
    }
  }

  var deltas = [];
  var highest = values[0];
  for (var i=1; i < values.length; i++) {
    var value = values[i];
    if (better(value, highest)) {
      if (highest.deltas) {
        deltas.concat(highest.deltas);
        delete highest["deltas"];
      }
      deltas.push(highest);
      highest = value;
    } else {
      if (value.deltas) {
        deltas.concat(value.deltas);
        delete value["deltas"];
      }
      deltas.push(value);
    }
  }

  var winrate = highest.stats.wins / highest.stats.count;
  var is_winrate_close = function(build) {
    var build_winrate = build.stats.wins / build.stats.count;
    return ((winrate - build_winrate) <= .05);
  }
  var playrate = highest.stats.count;
  for (var i=0; i < deltas.length;i++) playrate += deltas[i].playrate;
  highest.deltas = deltas.filter(is_winrate_close);
  highest.deltas = deltas.map(function(current) {
    return {
      finalBuild: current.finalBuild,
      wins: current.stats.wins,
      count: current.stats.count
    }
  });
  highest.playrate = playrate;
  return highest;
}
"""

ITEM_DELTA_MAP_FN = """
function() {
  var build_key = this.value.finalBuild.slice();
  build_key[_index] = "----";
  var key = {
    champion: this.value.championId,
    role: this.value.role,
    lane: this.value.lane,
    _key: build_key.join(",")
  }
  var value = this.value; 
  value.playrate = value.stats.count;
  value.deltas = [];
  emit(key, value);
}
"""

GROUP_MAP_FN = """
function() {
  var champBuild = {
    common: this.value,
    outliers: []
  }
  emit(this.value.championId, champBuild);
}
"""

GROUP_REDUCE_FN = """
function (id, builds) {
  var sortByPlayrate = function (champ_builds) {
    return champ_builds.sort(function(a, b) {
        if (a.common.playrate > b.common.playrate) return -1;
        else if (a.common.playrate === b.common.playrate) return 0;
        else return 1;
    });
  }

  var sortOutliers = function(builds) {
    return builds.sort(function(a, b) {
        var h1 = a.playrate * (a.stats.wins / a.stats.count);
        var h2 = b.playrate * (b.stats.wins / b.stats.count);
        if (h1 > h2) return -1;
        else if (h1 === h2) return 0;
        else return 1;
    });
  }

  var sorted_builds = sortByPlayrate(builds);
  var common = sorted_builds[0].common;
  var outliers = sorted_builds[0].outliers;
  for (var i=1; i < sorted_builds.length; i++) {
    var champBuilds = sorted_builds[i];
    outliers.concat(champBuilds.outliers);
    outliers.push(champBuilds.common);
  }
  
  var winrate = common.stats.wins / common.stats.count;
  var is_winrate_close = function(build) {
    var build_winrate = build.stats.wins / build.stats.count;
    return ((winrate - build_winrate) <= .05);
  }
  outliers = outliers.filter(is_winrate_close);

  return {common: common, outliers: sortOutliers(outliers)}
}

"""


def main(argv):
  mongo_url = "mongodb://localhost:27017"
  
  parser = argparse.ArgumentParser(description='Determine unique builds')
  parser.add_argument("-i", default="builds_consolidated", help="Collection to analyze from")
  parser.add_argument("-o", default="unique_builds", help="Output collection")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  input_coll = outliers_db[args.i]
  output_coll = outliers_db[args.o]

  output_coll.drop()
  output_coll.create_index("value.championId")

  print "Grouping for order deltas..."
  input_coll.map_reduce(ORDER_DELTA_MAP_FN, DELTA_REDUCE_FN, out=SON([('replace', args.o)]), sort={"value.championId": 1})
  for i in xrange(0, 6):
    print "Grouping for item deltas with index %d" % i
    output_coll.map_reduce(ITEM_DELTA_MAP_FN, DELTA_REDUCE_FN, out=SON([('replace', args.o)], sort={"value.championId": 1}),
      scope={"_index": i}
    )

  print "Grouping by champion and determining outliers..."
  output_coll.map_reduce(GROUP_MAP_FN, GROUP_REDUCE_FN, out=SON([('replace', args.o)]))


if __name__ == "__main__":
   main(sys.argv[1:])
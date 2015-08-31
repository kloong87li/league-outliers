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
  emit(key, this.value);
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
  for (var i=0; i < deltas.length;i++) playrate += (deltas[i].playrate || deltas[i].stats.count);
  highest.deltas = deltas.filter(is_winrate_close)
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

  emit(key, this.value);
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

  output_coll.drop();

  print "Grouping for order deltas..."
  input_coll.map_reduce(ORDER_DELTA_MAP_FN, DELTA_REDUCE_FN, out=SON([('replace', args.o)]))
  for i in xrange(0, 6):
    print "Grouping for item deltas with index %d" % i
    input_coll.map_reduce(ITEM_DELTA_MAP_FN, DELTA_REDUCE_FN, out=SON([('replace', args.o)]),
      scope={"_index": i}
    )



if __name__ == "__main__":
   main(sys.argv[1:])
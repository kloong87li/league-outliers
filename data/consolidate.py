import sys, argparse, threading
from pymongo import MongoClient
from bson import ObjectId
from bson import SON


def build_id_for_substr(start, length):
  # Generates the group by _id with the given substr of _key
  return {
    "subkey": {"$substr": ["$_key", start, length]},
    "champion": "$championId",
    "lane": "$lane",
    "role": "$role"
  }

def if_build_size_cond(field, size, cond, dummy=None):
  # Check if build size $cond size (i.e $eq, $ne, etc), if so adds the field value to the set,
  # otherwise adds dummy build to the set
  return {
    "$addToSet": {
      "$cond": {
        "if": {cond: [{"$size": "$finalBuild"}, size]},
        "then": field,
        "else": dummy,
      }
    }
  }

def if_build_size_eq(field, size, dummy=None):
  return if_build_size_cond(field, size, "$eq", dummy)

def if_build_size_neq(field, size, dummy=None):
  return if_build_size_cond(field, size, "$ne", dummy)

def get_partial_value(build_size):
  return {
    "build_size": {"$literal": build_size},
    "weight": "$_partial_stats_weight",
    "stats": {
      "count": "$_partial_stats_count",
      "wins": "$_partial_stats_wins",
      "losses": "$_partial_stats_losses",
      "kills": "$_partial_stats_kills",
      "deaths": "$_partial_stats_deaths",
      "assists": "$_partial_stats_assists",
      "damageToChampions": "$_partial_stats_damageToChampions",
      "minionsKilled":"$_partial_stats_minionsKilled",
      "goldEarned": "$_partial_stats_goldEarned",
    },
    "skillups": "$_partial_skillups",
    "summonerSpells": "$_partial_summonerSpells",
    "runes": "$_partial_runes",
    "masteries": "$_partial_masteries",
    "itemEvents": "$_partial_itemEvents"
  }

def reshape_partial(build_size, is_first_stage=False):
  # Reshapes document to be the original format, to be used after each consolidation
  partial_key = "size_" + str(build_size)
  if is_first_stage:
    return {
      "$project": {
        "_id": 0,
        "_original_id": {"$ifNull": ["$value._original_id", "$value._id"]},
        "_partials": {
          partial_key: get_partial_value(build_size),
        },
        "championId": "$value.championId",
        "lane": "$value.lane",
        "role": "$value.role",
        "skillups": "$value.skillups",
        "summonerSpells": "$value.summonerSpells",
        "runes": "$value.runes",
        "masteries": "$value.masteries",
        "itemEvents": "$value.itemEvents",
        "finalBuild": "$value.finalBuild",
        "_key": "$value._key",
        "stats": {
          "count": "$value.stats.count",
          "wins": "$value.stats.wins",
          "losses": "$value.stats.losses",
          "kills": "$value.stats.kills",
          "deaths": "$value.stats.deaths",
          "assists": "$value.stats.assists",
          "damageToChampions": "$value.stats.damageToChampions",
          "minionsKilled": "$value.stats.minionsKilled",
          "goldEarned": "$value.stats.goldEarned",
        }
      }
    }
  else:
    return {
      "$project": {
        "_id": 0,
        "_original_id": {"$ifNull": ["$value._original_id", "$value._id"]},
        "_partials": {
          partial_key: get_partial_value(build_size),
        },
        "finalBuild": "$value.finalBuild",
      }
    }

def group_builds(build_size):
  substr_len = 5 * build_size - 1
  dummy_build = {"isDummy": {"$literal":True}, "finalBuild": {"$literal": []}}
  return {
    "$group": {
      "_id": build_id_for_substr(0, substr_len),
      "_partial_stats_weight": {  # How many final builds this partial build is added to
        "$sum": {
          "$cond": {
            "if": {"$eq": [{"$size": "$finalBuild"}, 6]},
            "then": 1,
            "else": 0,
          }
        }
      },
      "_partial_stats_count": if_build_size_eq("$stats.count", build_size),
      "_partial_stats_wins": if_build_size_eq("$stats.wins", build_size),
      "_partial_stats_losses": if_build_size_eq("$stats.losses", build_size),
      "_partial_stats_kills": if_build_size_eq("$stats.kills", build_size),
      "_partial_stats_deaths": if_build_size_eq("$stats.deaths", build_size),
      "_partial_stats_assists": if_build_size_eq("$stats.assists", build_size),
      "_partial_stats_damageToChampions": if_build_size_eq("$stats.damageToChampions", build_size),
      "_partial_stats_minionsKilled":if_build_size_eq("$stats.minionsKilled", build_size),
      "_partial_stats_goldEarned": if_build_size_eq("$stats.goldEarned", build_size),
      "_partial_skillups": if_build_size_eq("$skillups", build_size),
      "_partial_summonerSpells": if_build_size_eq("$summonerSpells", build_size),
      "_partial_runes": if_build_size_eq("$runes", build_size),
      "_partial_masteries": if_build_size_eq("$masteries", build_size),
      "_partial_itemEvents": if_build_size_eq("$itemEvents", build_size),
      "value": if_build_size_neq("$$CURRENT", build_size, dummy=dummy_build)
    }
  }


def get_val_from_array_field(field, default=None):
  if not isinstance(field, list):
    return field  # already not an array
  for val in field:
    if val is not None:
      return val
  return default

def merge_dicts(fromd, intod):
  for key in fromd:
    if key in intod:
      intod[key] += fromd[key]
    else:
      intod[key] = fromd[key]

def merge_item_trie(fromt, intot):
  # Merge trie (fromt) to trie (intot)
  # Each input looks like:
  # {
  #   itemId1: {count, wins, is_final_item, neighbors}
  #   itemId2: ...
  #   ...
  # }
  for neighbor in fromt:
    if neighbor in intot:
      # Already exists in destination, combine count and recurse
      intot[neighbor]["count"] += fromt[neighbor]["count"]
      intot[neighbor]["wins"] += fromt[neighbor]["wins"]
      intot[neighbor]["timestamp"] += fromt[neighbor]["timestamp"]
      if "neighbors" in fromt[neighbor]:
        if "neighbors" in intot[neighbor]:
          merge_item_trie(fromt[neighbor]["neighbors"], intot[neighbor]["neighbors"])
        else:
          intot[neighbor]["neighbors"] = fromt[neighbor]["neighbors"]
    else:  # not in destination tree, move entire subtree over
      intot[neighbor] = fromt[neighbor]

def aggregate_partial(build, partial_build):
  # Sum stats
  for key in partial_build["stats"]:
    build["stats"][key] += get_val_from_array_field(partial_build["stats"][key], 0)

  # Merge runes, masteries, skillups, summonerSpells
  merge_dicts(get_val_from_array_field(partial_build["runes"], {}), build["runes"])
  merge_dicts(get_val_from_array_field(partial_build["masteries"], {}), build["masteries"])
  merge_dicts(get_val_from_array_field(partial_build["skillups"], {}), build["skillups"])
  merge_dicts(get_val_from_array_field(partial_build["summonerSpells"], {}), build["summonerSpells"])
  
  # Merge itemEvents tries
  partial_trie = get_val_from_array_field(partial_build["itemEvents"])
  if partial_trie is None:
    return
  merge_item_trie(partial_trie["neighbors"], build["itemEvents"]["neighbors"])


PARTIAL_MAP_FN = """
function() {
  var key = this._id;
  var value = this.value;
  emit(key, value);
}
"""

# Each reduce should only be between two values, the new
# partial result and the existing result in temp
PARTIAL_REDUCE_FN = """
function(key, values) {
  // Move partials
  var partials = {};
  var result = values[0];
  for (var i=0; i < values.length; i++) {
    var val = values[i]
    if (!val) continue;
    if (val._partials) {
      for (var key in val._partials) {
        partials[key] = val._partials[key];
      }
    }
    if (val.stats) result = val;
  }
  result._partials = partials;

  // Combine partial stats and stuff
  var get_val_from_array_field = function(field, default_val) {
    if (field.constructor !== Array) {
      return field;
    }
    for (var i=0;i<field.length;i++) {
      if (field[i]) return field[i];
    }
    return default_val;
  }

  var merge_dicts = function(fromd, intod, weight) {
    for (var key in fromd) {
      if (intod[key]){
        intod[key] += fromd[key] / weight;
      } else {
        intod[key] = fromd[key] / weight;
      }
    }
  }

  var merge_item_trie = function(fromt, intot) {
    for (var neighbor in fromt) {
      if (intot[neighbor]) {
        intot[neighbor]["count"] += fromt[neighbor]["count"];
        intot[neighbor]["wins"] += fromt[neighbor]["wins"];
        intot[neighbor]["timestamp"] += fromt[neighbor]["timestamp"];
        if (fromt[neighbor]["neighbors"]) {
          if (intot[neighbor]["neighbors"]){
            merge_item_trie(fromt[neighbor]["neighbors"], intot[neighbor]["neighbors"]);
          } else {
            intot[neighbor]["neighbors"] = fromt[neighbor]["neighbors"];
          }
        }
      } else {
        intot[neighbor] = fromt[neighbor];
      }
    }
  }

  var aggregate_partial = function(build, partial_build) {
    var weight = partial_build["weight"];
    for (var key in partial_build["stats"]) {
      var stat_val = get_val_from_array_field(partial_build["stats"][key], 0);
      build["stats"][key] += stat_val / weight;
    }

    merge_dicts(get_val_from_array_field(partial_build["runes"], {}), build["runes"], weight);
    merge_dicts(get_val_from_array_field(partial_build["masteries"], {}), build["masteries"], weight);
    merge_dicts(get_val_from_array_field(partial_build["skillups"], {}), build["skillups"], weight);
    merge_dicts(get_val_from_array_field(partial_build["summonerSpells"], {}), build["summonerSpells"], weight);
    
    var partial_trie = get_val_from_array_field(partial_build["itemEvents"]);
    if (!partial_trie) {
      return;
    }
    merge_item_trie(partial_trie["neighbors"], build["itemEvents"]["neighbors"]);
  }

  var partials = result._partials;
  for (var partial_key in partials) {
    aggregate_partial(result, partials[partial_key]);
  }
  delete result["_partials"];
  delete result["_original_id"];

  return result;
}
"""

FINALIZE_MAP_FN = """
function() {
  var build = this.value;

  var find_highest = function(obj) {
    var highest_val = -1;
    var highest_key = null;
    for (var key in obj) {
      if (obj[key] > highest_val) {
        highest_val = obj[key];
        highest_key = key;
      }
    }
    return highest_key;
  }

  build["runes"] = find_highest(build["runes"]);
  build["skillups"] = find_highest(build["skillups"]);
  build["masteries"] = find_highest(build["masteries"]);

  var highest = find_highest(build["summonerSpells"]);
  delete build["summonerSpells"][highest];
  build["summonerSpells"] = [highest, find_highest(build["summonerSpells"])];

  var sortByCountThenWinrate = function (events) {
    return events.sort(function(a, b) {
        var c1 = parseInt(a.count); var c2 = parseInt(b.count);
        var w1 = parseInt(a.wins); var w2 = parseInt(b.count);
        if (c1 > c2) {
          return -1;
        } else if (c1 === c2) {
          return ((w1/c1) > (w2/c2)) ? -1 : 1;
        } else {
          return 1;
        }
    });
  }

  var find_path = function (neighbors, final_items_left) {
    var neighbors_array = [];
    for (var key in neighbors){
      neighbors_array.push(neighbors[key]);
    }
    var sorted_neighbors = sortByCountThenWinrate(neighbors_array);
    for (var i=0; i < sorted_neighbors.length; i++) {
      var neighbor = sorted_neighbors[i];
      if (neighbor.is_final_item) final_items_left--;
      if (neighbor.neighbors) {
        var path = find_path(neighbor.neighbors, final_items_left);
        if (path) {
          path.unshift({
            is_final_item: neighbor.is_final_item,
            itemId: neighbor.itemId,
            timestamp: neighbor.timestamp / neighbor.count
          })
          return path;
        }
      } else {
        return (final_items_left > 0) ? null : [{
          is_final_item: neighbor.is_final_item,
          itemId: neighbor.itemId,
          timestamp: neighbor.timestamp / neighbor.count,
        }];
      }
    }
  }

  var item_path = find_path(build["itemEvents"]["neighbors"], 6);
  assert(item_path !== null);
  build["itemEvents"] = item_path;
  
  emit(this._id, build);
}

"""

FINALIZE_REDUCE_FN = """
function(id, builds) {
  assert(builds.length === 1);
  return builds[0];
}

"""


def main(argv):
  mongo_url = "mongodb://localhost:27017"
  
  parser = argparse.ArgumentParser(description='Aggregate player builds by champion, build, and role')
  parser.add_argument("-i", default="builds", help="Collection to aggregate from")
  parser.add_argument("-o", default="builds_consolidated", help="Output collection")
  parser.add_argument("--temp", default="temp", help="Temp data collection")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  input_coll = outliers_db[args.i]
  temp_coll = outliers_db[args.temp]
  output_coll = outliers_db[args.o]

  # Build pipeline
  def pipeline(build_size, is_first_stage=False):
    return [
      {
        "$match": {"$or": [
          {"finalBuild": {"$size": 6}},
          {"finalBuild": {"$size": build_size}}
        ]}
      },
      group_builds(build_size),
      { "$unwind": "$value"},
      reshape_partial(build_size, is_first_stage),
      # Filter out builds of other sizes and regroup by id
      {"$match": {"finalBuild": {"$size": 6}}},
      {"$group": {"_id": "$_original_id", "value": {"$first": "$$CURRENT"}}},
      { "$out" :  args.o if is_first_stage else args.temp}
    ]

  # Reset output
  print "Reseting output collections..."
  output_coll.drop()
  temp_coll.drop()
  output_coll.create_index("value.championId")

  # Perform aggregation
  for i in xrange (5, 1, -1):
    print "Aggregating builds with size: %d" % i
    input_coll.aggregate(pipeline(i, i==5), allowDiskUse=True)
    if i != 5:
      print "Merging with output collection through map-reduce..."
      temp_coll.map_reduce(PARTIAL_MAP_FN, PARTIAL_REDUCE_FN, out=SON([('reduce', args.o)]),
        sort={"_id": 1})

  print "Finalizing results via map-reduce..."
  output_coll.map_reduce(FINALIZE_MAP_FN, FINALIZE_REDUCE_FN, out=SON([('replace', args.o)]),
      sort={"_id": 1})

  output_coll.delete_many({"value.itemEvents": None})

  print "Done!"
  print "...dropping temp collections."
  temp_coll.drop()

if __name__ == "__main__":
   main(sys.argv[1:])
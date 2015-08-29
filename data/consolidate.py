import sys, argparse, threading
from pymongo import MongoClient
from bson import ObjectId


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
            "if": {cond: [{"$size": "$finalBuild"}, 6]},
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

def process_build(bulk, build_doc):
  build = build_doc["value"]
  stats = build["stats"]
  partials = build["_partials"]
  for partial_key in partials:
    aggregate_partial(build, partials[partial_key])

  del build["_partials"]
  bulk.find(
    {"_id": build_doc["_id"]}
  ).upsert().replace_one(
    {
      "_id": build_doc["_id"],
      "value": build
    }
  )

def consolidate_partials(temp, output, num_cursors):
  def process_cursor(cursor):
    bulk = output.initialize_unordered_bulk_op()
    for document in cursor:
      process_build(bulk, document)
    bulk.execute()

  cursors = temp.parallel_scan(num_cursors)
  threads = [
    threading.Thread(target=process_cursor, args=(cursor,))
    for cursor in cursors
  ]

  for thread in threads:
    thread.start()

  for thread in threads:
    thread.join()

def relocate_build_with_partials(build_size, bulk, build):
  partial_key = "size_" + str(build_size)
  partial_path = "value._partials." + partial_key
  partial_value = build["value"]["_partials"][partial_key]
  value = build["value"]
  bulk.find(
    {"_id": build["_id"]}
  ).upsert().update_one(
    {
      "$set": {partial_path: partial_value}
    }
  )

def relocate_temp(build_size, temp1, temp2, num_cursors):
  def process_cursor(cursor):
    bulk = temp2.initialize_unordered_bulk_op()
    for document in cursor:
      relocate_build_with_partials(build_size, bulk, document)
    bulk.execute()

  cursors = temp1.parallel_scan(num_cursors)
  threads = [
    threading.Thread(target=process_cursor, args=(cursor,))
    for cursor in cursors
  ]

  for thread in threads:
    thread.start()

  for thread in threads:
    thread.join()

MAP_FN = """
function() {
  var key = this._id;
  var value = this.value;
  emit(key, value);
}
"""

# Each reduce should only be between two values, the new
# partial result and the exsiting result in temp
REDUCE_FN = """
function(key, values) {
  var partials = {};
  var result = values[0];
  for (var val in values) {
    if (val._partials) {
      for (var key in val._partials) {
        partials[key] = val._partials[key];
      }
    } else {
      result = val;
    }
  }
  result._partials = partials
  return result;
}
"""

# Merge partials into final stats/data structures
FINALIZE_FN = """
function(key, reducedValue) {
  
  return reducedValue;
}
"""

def main(argv):
  mongo_url = "mongodb://localhost:27017"
  
  parser = argparse.ArgumentParser(description='Aggregate player builds by champion, build, and role')
  parser.add_argument("-i", default="builds", help="Collection to aggregate from")
  parser.add_argument("-o", default="builds_consolidated", help="Output collection")
  parser.add_argument("-n", default=4, type=int, help="Number of threads used to consolidate builds")
  parser.add_argument("--temp", default="_temp", help="Temp data collection")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  input_coll = outliers_db[args.i]
  temp1_coll = outliers_db[args.temp+"_1"]
  temp2_coll = outliers_db[args.temp+"_2"]
  output_coll = outliers_db[args.o]

  # Build pipeline
  def pipeline(build_size, is_first_stage=False):
    temp_suffix = "_2" if is_first_stage else "_1"
    return [
      {
        "$match": {"$or": [{"finalBuild": {"$size": 6}}, {"finalBuild": {"$size": build_size}}]}
      },
      group_builds(build_size),
      { "$unwind": "$value"},
      reshape_partial(build_size, is_first_stage),
      {"$match": {"finalBuild": {"$size": 6}}},  # Filter out builds of other sizes
      {"$group": {"_id": "$_original_id", "value": {"$first": "$$CURRENT"}}},
      { "$out" :  args.temp + temp_suffix}
    ]

  # Reset output
  print "Reseting output collections..."
  output_coll.drop()
  temp1_coll.drop()
  temp2_coll.drop()

  # Perform aggregation
  for i in xrange(5, 0, -1):
    print "Aggregating builds with size: %d" % i
    if i == 5:
      input_coll.aggregate(pipeline(i, True), allowDiskUse=True)
    else:
      input_coll.aggregate(pipeline(i, False), allowDiskUse=True)
      print "Relocating to temporary..."
      relocate_temp(i, temp1_coll, temp2_coll, args.n)

  # Consolidate partial data
  print "Consolidating partials..."
  consolidate_partials(temp2_coll, output_coll, args.n)

  print "Done!"
  print "...dropping temp collections."
  temp1_coll.drop()
  temp2_coll.drop()

if __name__ == "__main__":
   main(sys.argv[1:])
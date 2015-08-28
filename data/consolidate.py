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

def get_partial_value(build_size, size_field):
  if build_size == size_field:
    return {
      "build_size": {"$literal": build_size},
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
  else:
    return "$value._partials.size_" + str(size_field)

def reshape_partial(build_size):
  # Reshapes document to be the original format, to be used after each consolidation
  return {
    "$project": {
      "_id": 0,
      "_original_id": {"$ifNull": ["$value._original_id", "$value._id"]},
      "_partials": {
        "size_5": get_partial_value(build_size, 5),
        "size_4": get_partial_value(build_size, 4),
        "size_3": get_partial_value(build_size, 3),
        "size_2": get_partial_value(build_size, 2),
        "size_1": get_partial_value(build_size, 1)
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

def group_builds(build_size):
  substr_len = 5 * build_size - 1
  dummy_build = {"isDummy": {"$literal":True}, "finalBuild": {"$literal": []}}
  return {
    "$group": {
      "_id": build_id_for_substr(0, substr_len),
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
      merge_item_trie(fromt[neighbor]["neighbors"], intot[neighbor]["neighbors"])
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

def process_build(output, build_doc):
  build = build_doc["value"]
  stats = build["stats"]
  partials = build["_partials"]
  for partial_key in partials:
    aggregate_partial(build, partials[partial_key])

  del build["_original_id"]
  del build["_partials"]
  output.replace_one(
    {"_id": build_doc["_id"]},
    {
      "_id": build_doc["_id"],
      "value": build
    },
    upsert=True
  )


def consolidate_partials(temp, output, num_cursors):
  def process_cursor(cursor):
    for document in cursor:
      process_build(output, document)

  cursors = temp.parallel_scan(num_cursors)
  threads = [
    threading.Thread(target=process_cursor, args=(cursor,))
    for cursor in cursors
  ]

  for thread in threads:
    thread.start()

  for thread in threads:
    thread.join()


def main(argv):
  mongo_url = "mongodb://localhost:27017"
  
  parser = argparse.ArgumentParser(description='Aggregate player builds by champion, build, and role')
  parser.add_argument("-i", default="builds", help="Collection to aggregate from")
  parser.add_argument("-o", default="builds_consolidated", help="Output collection")
  parser.add_argument("-n", default=4, help="Number of threads used to consolidate builds")
  parser.add_argument("--temp", default="_temp", help="Temp data collection")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  input_coll = outliers_db[args.i]
  temp_coll = outliers_db[args.temp]
  output_coll = outliers_db[args.o]

  # Build pipeline
  pipeline = []
  for i in xrange(5, 0, -1):
    # For each build size, group to compute partials,
    # unwind, and move partial data to _partials field
    pipeline.append(group_builds(i))
    pipeline.append({ "$unwind": "$value"})
    pipeline.append(reshape_partial(i))
  
  pipeline.append({"$match": {"finalBuild": {"$size": 6}}})  # Filter out builds of other sizes
  pipeline.append(  # Regroup to reset _id
    {"$group": {"_id": "$_original_id", "value": {"$first": "$$CURRENT"}}},
  )
  pipeline.append({ "$out" : args.temp })  # Output to DB

  # Perform aggregation
  print "Aggregating..."
  input_coll.aggregate(pipeline, allowDiskUse=True)

  # Consolidate partial data
  print "Consolidating partials..."
  consolidate_partials(temp_coll, output_coll, args.n)

  print "Done!"
  print "...dropping temp collection."
  temp_coll.drop()

if __name__ == "__main__":
   main(sys.argv[1:])
from util import datetime_to_timestamp

import sys, argparse, threading, time
from pymongo import MongoClient
from bson import ObjectId
from bson.code import Code

MAP_FN = """
function() {
  var key = {
    championId: this.participant.championId,
    role: this.participant.timeline.role,
    lane: this.participant.timeline.lane,
    buildKey: this.finalBuild.join(",")
  };

  var stats = this.participant.stats;
  var value = {
    buildIds: [this._id],
    finalBuild: this.finalBuild,
    itemEvents: this.itemEvents,
    skillUps: this.skillUps,
    summonerSpells: [this.participant.spell1Id, this.participant.spell2Id],
    runes: this.participant.runes,
    masteries: this.participant.masteries,
    stats: {
      count: 1,
      kills: stats.kills,
      deaths: stats.deaths,
      assists: stats.assists,
      damageToChampions: stats.totalDamageDealtToChampions,
      minionsKilled: stats.minionsKilled,
      goldEarned: stats.goldEarned,
      wins: stats.winner ? 1 : 0,
      losses: stats.winner ? 0 : 1
    }
  };
  emit(key, value);
}
"""

REDUCE_FN = """
function(key, values) {
  result = values[0];
  for (var i=1; i < values.length; i++) {
    result.buildIds.concat(values[i].buildIds)
    for (var key in result.stats) {
      result.stats[key] = result.stats[key] + values[i].stats[key];
    }
  }
  return result;
}
"""

FINALIZE_FN = """
function(key, reducedValue) {
  reducedValue.winRate = reducedValue.stats.wins / reducedValue.stats.count;
  return reducedValue;
}
"""

def main(argv):
  mongo_url = "mongodb://localhost:27017"
  
  parser = argparse.ArgumentParser(description='Aggregate player builds by champion, build, and role')
  parser.add_argument("-t", default=0, help="Timestamp (seconds) to fetch data for. Includes all builds with timestamp > t")
  parser.add_argument("-c", default="player_builds", help="Collection to aggregate")
  parser.add_argument("-o", default="build_stats", help="Output collection")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  input_coll = outliers_db[args.c]

  # Get object id based on time stamp
  def seconds_to_oid(secs):
    hex_seconds = hex(secs)
    hex_string = "0x" + (hex_seconds[2:]).zfill(8)
    hex_string = (hex_string + "0000000000000000")[2:]
    return ObjectId(hex_string)

  start = int(args.t)
  start_oid = seconds_to_oid(start)
  now = int(time.time())
  end_oid = seconds_to_oid(now)

  print "Map-Reducing between: %r - %r" % (start, now)
  query = {"_id": {"$gt": start_oid, "$lt": end_oid}}
  input_coll.map_reduce(MAP_FN, REDUCE_FN, args.o, finalize=FINALIZE_FN, reduce_output=True, query=query)


if __name__ == "__main__":
   main(sys.argv[1:])
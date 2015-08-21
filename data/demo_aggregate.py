from util import datetime_to_timestamp

import sys, argparse, threading, time
from pymongo import MongoClient
from bson import ObjectId
from bson.code import Code

MAP_FN = """
function() {
  var value = this.value
  if (value.finalBuild.length < 6) {
    return
  }
  var finalBuild = value.finalBuild
  var sorted = value.finalBuild.slice().sort()
  var key = {
    championId: this._id.championId,
    role: this._id.role,
    lane: this._id.lane,
    buildKey: sorted.join(",")
  };

  this._id = key;
  value.sortedBuild = sorted
  emit(key, this.value);
}
"""

REDUCE_FN = """
function(key, values) {
  var result = values[0];
  for (var i=1; i < values.length; i++) {
    result.buildIds.concat(values[i].buildIds)
    for (var k in result.stats) {
      result.stats[k] = result.stats[k] + values[i].stats[k];
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

  # Initialize MongoDB
  mongo_client = MongoClient(mongo_url)
  outliers_db = mongo_client.outliers

  input_coll = outliers_db["build_stats"]

  input_coll.map_reduce(MAP_FN, REDUCE_FN, "demo_build_stats", finalize=FINALIZE_FN, reduce_output=False)


if __name__ == "__main__":
   main(sys.argv[1:])
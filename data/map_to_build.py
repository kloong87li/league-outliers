from util import MatchProcessor
from riot_api import RiotItems, RiotApi

import sys, argparse, threading
from pymongo import MongoClient

def translate_build(riot_items, items):
  print [riot_items.get_item(str(item))["name"] for item in items]

def main(argv):
  mongo_url = "mongodb://localhost:27017"
  collection = "matches"

  parser = argparse.ArgumentParser(description='Map match id to builds')
  parser.add_argument("-t", default="any", help="Test single input. 'any' for random match (default)")
  parser.add_argument("-c", default=collection, help="Collection to process")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  # Initialize components
  riot_items = RiotItems()
  match_processor = MatchProcessor(riot_items)
  input_db = outliers_db[args.c]

  # Test script
  if args.t:
    match_id = args.t
    if args.t == 'any':
      match_id = input_db.find_one()["matchId"]

    match = RiotApi.get_match(match_id)
    print "Processing Match: %d" % match["matchId"]
    trimmed = match_processor.trim_match(match)
    participants = match_processor.get_builds_from_match(match)

    print "[Trimmed]:"
    print trimmed
    print "[Builds]:"
    for p in participants:
      translate_build(riot_items, p["build"]["finalBuild"])
    return

if __name__ == "__main__":
   main(sys.argv[1:])
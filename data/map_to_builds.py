from util import MatchProcessor
from riot_api import RiotItems

import sys, argparse, threading
from pymongo import MongoClient

def translate_build(riot_items, items):
  print [riot_items.get_item(str(item))["name"] for item in items]

def main(argv):
  mongo_url = "mongodb://localhost:27017"
  collection = "raw_matches"
  output = "matches"

  parser = argparse.ArgumentParser(description='Map matches in database to builds')
  parser.add_argument("-t", default=None, help="Test single input. 'any' for random match")
  parser.add_argument("-c", default=collection, help="Collection to process")
  parser.add_argument("-o", default=collection, help="Collection to output to")
  parser.add_argument("-n", default=4, help="Number of threads to use.")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  args = parser.parse_args()

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  # Initialize components
  riot_items = RiotItems()
  match_processor = MatchProcessor(riot_items)
  matches_db = outliers_db[args.c]

  # Test script
  if args.t:
    match = None
    if args.t == 'any':
      query = {"is_ref": False}
    else:
      query = {"matchId": args.t}

    match = matches_db.find_one(query)

    print "Processing Match: %d" % match["matchId"]
    trimmed = match_processor.trim_match(match)
    builds = match_processor.get_builds_from_match(match)

    print "[Trimmed]:"
    print trimmed
    print "[Builds]:"
    for build in builds:
      translate_build(riot_items, build["finalBuild"])
    return

  # Process all matches in DB
  cursors = matches_db.parallel_scan(4)
  threads = [
    threading.Thread(target=process_cursor, args=(cursor,))
    for cursor in cursors
  ]
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()


if __name__ == "__main__":
   main(sys.argv[1:])
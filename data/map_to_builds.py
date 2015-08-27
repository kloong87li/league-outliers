from util import MatchProcessor
from riot_api import RiotItems, RiotApi

import sys, argparse, threading
from pymongo import MongoClient

def translate_build(riot_items, items):
  print [riot_items.get_item(str(item))["name"] for item in items]

def main(argv):
  mongo_url = "mongodb://localhost:27017"
  collection = "matches"
  output = "matches"

  parser = argparse.ArgumentParser(description='Map matches in database to builds')
  parser.add_argument("-t", default=None, help="Test single input. 'any' for random match")
  parser.add_argument("-c", default=collection, help="Collection to process")
  parser.add_argument("-om", default=output, help="Collection to output matches to")
  parser.add_argument("-ob", default="player_builds", help="Collection to output builds to")
  parser.add_argument("-v", action="store_true", default=False, help="Verbose")

  parser.add_argument("-n", default=4, help="Number of threads to use.")
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


  # Process all matches in DB using parallel scan

  # Function to process each cursor
  # m_output = outliers_db[args.om]
  # b_output = outliers_db[args.ob]
  # def process_cursor(cursor):
  #   try:
  #     for match in cursor:
  #       try:
  #         if "is_ref" in match and match["is_ref"]:  # Match not yet fetched
  #           continue

  #         trimmed = match_processor.trim_match(match)
  #         player_builds = match_processor.get_builds_from_match(match)
  #         result = b_output.insert_many(player_builds)
  #         trimmed["participantStats"] = result.inserted_ids

  #         if args.v:
  #           print "Inserted builds for match: %r" % match["matchId"]
  #         m_output.replace_one(
  #           {"matchId": match["matchId"]},
  #           trimmed,
  #           upsert=True
  #         )
  #       except Exception as e:
  #         print "!! Exception occurred for match: %d, marking as error. (%r)" % (match["matchId"], e)
  #         m_output.replace_one(
  #           {"matchId": match["matchId"]},
  #           {
  #             "matchId": match["matchId"],
  #             "error": repr(e)
  #           },
  #           upsert=True
  #         )
  #         continue
  #   except Exception as e:
  #     print "!! Exception occurred in thread, terminating: %r" % e

  # cursors = input_db.parallel_scan(int(args.n))
  # threads = [
  #   threading.Thread(target=process_cursor, args=(cursor,))
  #   for cursor in cursors
  # ]
  # for thread in threads:
  #   thread.start()
  # for thread in threads:
  #   thread.join()


if __name__ == "__main__":
   main(sys.argv[1:])
#!/usr/bin/python

from riot_api import RiotApiScheduler, ApiRequest, RiotApi, API_KEY
from workers import PlayerWorker, MatchWorker
from db import PlayerDb, MatchDb
from util import datetime_to_timestamp

import signal
import sys
import argparse
import time
import datetime
from Queue import Queue

from pymongo import MongoClient

MAX_PLAYER_QSIZE = 500
MAX_MATCH_QSIZE = 1500

def main(argv):
  mongo_url = "mongodb://localhost:27017"
  last_update = datetime_to_timestamp(PlayerDb.EARLIEST_UPDATE) + 1
  initial_seed = 49159160
  update_old = False
  workers = [1, 1]

  # Parse commandline for last_update date, and thread numbers
  parser = argparse.ArgumentParser(description='Collect data from Riot API')
  parser.add_argument("-i", default=initial_seed, type=int, help="Initial summoner ID")
  parser.add_argument("-n", default=workers, nargs=2, type=int, help="# of pworkers and mworkers, respectively")
  parser.add_argument("-d", help="Starting date to fetch data for")   # TODO add commandline args
  parser.add_argument("--update_old", action='store_true', help="Whether to prioritize gathering of new player data")
  parser.add_argument("--mongo", default=mongo_url, help="URL of MongoDB")
  parser.add_argument("--api_key", default=API_KEY, help="Riot API Key")
  args = parser.parse_args()

  if args.d is not None:
    last_update = datetime_to_timestamp(datetime.datetime.strptime(args.d, "%m/%d/%y"))
  if args.api_key == "":
    print "API key not found. Please add your key to api_key.py or use --api_key"
    sys.exit(2)
  else:
    RiotApi.set_api_key(args.api_key)

  # Initialize MongoDB
  mongo_client = MongoClient(args.mongo)
  outliers_db = mongo_client.outliers

  # Initialize components
  api_scheduler = RiotApiScheduler()
  player_db = PlayerDb(outliers_db.players)
  match_db = MatchDb(outliers_db.raw_matches)
  player_queue = Queue(maxsize=MAX_PLAYER_QSIZE)
  match_queue = Queue(maxsize=MAX_MATCH_QSIZE)
  workers = []

  # Initial seed
  player_queue.put({
      "profileIcon": 588,
      "matchHistoryUri": "/v1/stats/player_history/NA/36821626",
      "summonerName": "-INITIAL_SEED-",
      "summonerId": args.i,
      "last_update": datetime_to_timestamp(PlayerDb.EARLIEST_UPDATE),
      "league": "GOLD"
   })

  # Register stop signal handler
  def stop(sig, frame):
    api_scheduler.stop()
    for worker in workers:
      worker.stop()
    print "[STOP] Players left: %r, Matches left: %r" % (player_queue.qsize(), match_queue.qsize())
  signal.signal(signal.SIGINT, stop)

  # Start threads
  api_scheduler.start()

  for i in xrange(args.n[0]):
    worker = PlayerWorker(
      is_prioritize_new=not args.update_old,
      api_scheduler=api_scheduler,
      player_db=player_db,
      match_db=match_db,
      last_update=last_update,
      player_queue=player_queue,
      match_queue=match_queue,
    )
    workers.append(worker)
    worker.start()

  for i in xrange(args.n[1]):
    worker = MatchWorker(
      last_update=last_update,
      api_scheduler=api_scheduler,
      player_db=player_db,
      match_db=match_db,
      player_queue=player_queue,
      match_queue=match_queue,
    )
    workers.append(worker)
    worker.start()

  # Janky way to quit program
  line = raw_input()
  if line == "q":
    stop(2, None)

if __name__ == "__main__":
   main(sys.argv[1:])

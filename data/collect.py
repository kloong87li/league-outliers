from riot_api import RiotApiScheduler, ApiRequest
from workers import PlayerWorker, MatchWorker
from db import PlayerDb, MatchDb
from util import datetime_to_timestamp

import signal
import sys, getopt
import time
from Queue import Queue

from pymongo import MongoClient

MAX_PLAYER_QSIZE = 100

def main(argv):
  # Parse commandline for last_update date, and thread numbers
  date = datetime_to_timestamp(PlayerDb.EARLIEST_UPDATE) + 1
  try:
    opts, args = getopt.getopt(argv,"d",["date="])
  except getopt.GetoptError:
    print 'test.py -d <date>'
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-d", "--date"):
       date = arg

  # Initialize MongoDB
  mongo_client = MongoClient("mongodb://localhost:27017")
  outliers_db = mongo_client.outliers

  # Initialize components
  api_scheduler = RiotApiScheduler()
  player_db = PlayerDb(outliers_db.players)
  match_db = MatchDb(outliers_db.matches)
  player_queue = Queue(maxsize=MAX_PLAYER_QSIZE)
  match_queue = Queue()
  workers = []

  # Initial seed
  player_queue.put({
      "profileIcon": 588,
      "matchHistoryUri": "/v1/stats/player_history/NA/36821626",
      "summonerName": "idunnololz",
      "summonerId": 22884498
   })

  # Register stop signal handler
  def stop(sig, frame):
    print "STOPPING"
    api_scheduler.stop()
    for worker in workers:
      worker.stop()
  signal.signal(signal.SIGINT, stop)

  # Start threads
  api_scheduler.start()

  for i in xrange(2):
    worker = PlayerWorker(
      is_prioritize_new=False,
      api_scheduler=api_scheduler,
      player_db=player_db,
      last_update=date,
      player_queue=player_queue,
      match_queue=match_queue,
    )
    workers.append(worker)
    worker.start()

  for i in xrange(2):
    worker = MatchWorker(
      api_scheduler=api_scheduler,
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

from pymongo.collection import ReturnDocument
import datetime

from util import datetime_to_timestamp

class PlayerDb(object):
  EARLIEST_UPDATE = datetime.datetime(2015, 8, 1)

  def __init__(self, player_collection):
    self._players = player_collection
    return

  def update_matches(self, player, timestamp):
    print "[PLAYERS] Updated player %r with last update time of %r" % (player["summonerName"], timestamp)
    self._players.update_one(
      {"summonerId": player["summonerId"]},
      {"$set": {"last_update": timestamp}}
    )

  def find_stale(self, last_update):
    return self._players.find_one({
      "last_update": {"$lt": last_update}
    })

  def find_or_create(self, player):
    print "[PLAYERS] Finding or updating player %r" % player["summonerName"]
    old = self._players.find_one({"summonerId": player["summonerId"]})
    if old is None:
      old = {
        "last_update": datetime_to_timestamp(PlayerDb.EARLIEST_UPDATE),
        "match_list": []
      }

    player.update({
      "last_update": old["last_update"],
      "match_list": old["match_list"]
    })

    self._players.replace_one(
      {"summonerId": player["summonerId"]},
      player,
      upsert=True
    )
    return player
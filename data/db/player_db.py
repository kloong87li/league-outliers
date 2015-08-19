from pymongo.collection import ReturnDocument
import datetime

from util import datetime_to_timestamp

class PlayerDb(object):
  EARLIEST_UPDATE = datetime.datetime(2015, 8, 15)

  def __init__(self, player_collection):
    self._players = player_collection
    return

  def update_matches(self, player, timestamp):
    self._players.update_one(
      {"summonerId": player["summonerId"]},
      {"$set": {"last_update": timestamp}}
    )

  def find_stale(self, last_update):
    return self._players.find_one_and_update(
      {"last_update": {"$lt": last_update}},
      {"$set": {"last_update": last_update}}
    )

  def find_or_create(self, player):
    old = self._players.find_one({"summonerId": player["summonerId"]})
    if old is None:
      old = {
        "last_update": datetime_to_timestamp(PlayerDb.EARLIEST_UPDATE),
      }

    player.update({
      "last_update": old["last_update"],
    })

    self._players.replace_one(
      {"summonerId": player["summonerId"]},
      player,
      upsert=True
    )
    return player



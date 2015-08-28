from pymongo.collection import ReturnDocument
from pymongo import ASCENDING

class BuildDb(object):

  def __init__(self, db):
    self._db = db
    self._db.builds.create_index([
      ("_key", ASCENDING),
      ("championId", ASCENDING),
      ("lane", ASCENDING),
      ("role", ASCENDING)
    ])
    self._db.runes.create_index("_key")
    self._db.masteries.create_index("_key")
    self._db.skillups.create_index("_key")
    return

  def insert_runes(self, runes):
    _key = "".join([str((r["runeId"], r["rank"])) for r in runes])
    result = self._db.runes.find_one_and_update(
      {"_key": _key},
      {"$setOnInsert": {"_key": _key, "value": runes}},
      upsert=True,
      return_document=ReturnDocument.AFTER
    )
    return str(result["_id"])

  def insert_masteries(self, masteries):
    _key = "".join([str((m["masteryId"], m["rank"])) for m in masteries])
    result = self._db.masteries.find_one_and_update(
      {"_key": _key},
      {"$setOnInsert": {"_key": _key, "value": masteries}},
      upsert=True,
      return_document=ReturnDocument.AFTER
    )
    return str(result["_id"])

  def insert_skillups(self, skillups):
    _key = "".join(skillups)
    result = self._db.skillups.find_one_and_update(
      {"_key": _key},
      {"$setOnInsert": {"_key": _key, "value": skillups}},
      upsert=True,
      return_document=ReturnDocument.AFTER
    )
    return str(result["_id"])

  def _get_item_trie_paths(self, itemEvents):
    path = "itemEvents"
    item_paths = []
    for event in itemEvents:
      path = path + (".neighbors.%s" % event["itemId"])
      path_obj = {
        "path": path,
        "itemId": event["itemId"],
        "is_final_item": "is_final_item" in event and event["is_final_item"]
      }
      item_paths.append(path_obj)
    return item_paths

  def insert_build(self, participant):
    build = participant["build"]
    stats = participant["stats"]
    _key = ",".join(build["finalBuild"])
    _key_sorted = ",".join(sorted(build["finalBuild"]))

    skillups_key = "skillups." + build["skillups"]
    spell1_key = "summonerSpells." + str(build["summonerSpells"][0])
    spell2_key = "summonerSpells." + str(build["summonerSpells"][1])
    runes_key = "runes." + build["runes"]
    masteries_key = "masteries." + build["masteries"]

    # Update param for skillups, masteries, runes, and summoner spells
    update_param = {
      "$setOnInsert": {
        "finalBuild": build["finalBuild"],
        "_key_sorted": _key_sorted,
      },
      "$inc": {
        skillups_key: 1,
        spell1_key: 1,
        spell2_key: 1,
        runes_key: 1,
        masteries_key: 1,
        "stats.count": 1,
        "stats.wins": 1 if stats["winner"] else 0,
        "stats.losses": 0 if stats["winner"] else 1,
        "stats.kills": stats["kills"],
        "stats.deaths": stats["deaths"],
        "stats.assists": stats["assists"],
        "stats.damageToChampions": stats["totalDamageDealtToChampions"],
        "stats.minionsKilled": stats["minionsKilled"],
        "stats.goldEarned": stats["goldEarned"]
      }
    }

    # Generate update param for item trie
    item_paths = self._get_item_trie_paths(build["itemEvents"])
    for path_obj in item_paths:
      if path_obj["is_final_item"]:
        update_param["$setOnInsert"][path_obj["path"] + ".is_final_item"] = True
      update_param["$setOnInsert"][path_obj["path"] + ".itemId"] = path_obj["itemId"]
      update_param["$inc"][path_obj["path"] + ".count"] = 1
      update_param["$inc"][path_obj["path"] + ".wins"] = 1 if stats["winner"] else 0

    # Make update
    result = self._db.builds.find_one_and_update(
      {
        "championId": build["championId"],
        "lane": build["lane"],
        "role": build["role"],
        "_key": _key
      },
      update_param,
      upsert=True,
      return_document=ReturnDocument.AFTER
    )
    return str(result["_id"])

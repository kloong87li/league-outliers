from pymongo.collection import ReturnDocument

class BuildDb(object):

  def __init__(self, db):
    self._db = db
    self._db.builds.create_index("_key")
    self._db.runes.create_index("_key")
    self._db.masteries.create_index("_key")
    self._db.skillups.create_index("_key")
    return

  def insert_item_events(self, itemEvents):
    result = self._db.item_events.insert_one(itemEvents)
    return str(result.inserted_id)

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

    result = self._db.builds.find_one_and_update(
      {
        "championId": build["championId"],
        "lane": build["lane"],
        "role": build["role"],
        "_key": _key
      },
      {
        "$setOnInsert": {"finalBuild": build["finalBuild"], "_key_sorted": _key_sorted},
        "$push": {"itemEvents": build["itemEvents"]},
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
      },
      upsert=True,
      return_document=ReturnDocument.AFTER
    )
    return str(result["_id"])


class MatchProcessor(object):

  def __init__(self, itemDb):
    self._itemDb = itemDb

  def trim_match(self, match):
    cmatch = {
      "region": match["region"],
      "matchMode": match["matchMode"],
      "matchCreation": match["matchCreation"],
      "matchVersion": match["matchVersion"],
      "mapId": match["mapId"],
      "season": match["season"],
      "matchId": match["matchId"],
      "matchDuration": match["matchDuration"],
      "queueType": match["queueType"],
      "teams": match["teams"],
      "participants": None,  # list of database ids
      "is_ref": False
    }

    return cmatch

  def _is_item_final(self, iid):
    return self._itemDb.is_final_item(iid)

  def _is_item_upgrade(self, iid):
    return self._itemDb.is_upgraded_item(iid)

  def _update_items_removed(self, iid, pid, items_removed):
    if self._is_item_final(iid):
      removed = items_removed[pid]
      if iid in removed:
        removed[iid] += 1
      else:
        removed[iid] = 1

  def _register_item_upgrade_sold(self, iid, pid, items_sold):
    item = self._itemDb.get_item(iid)
    if item and "from" in item:
      for base_iid in item["from"]:
        self._update_items_removed(base_iid, pid, items_sold)

  def _process_item_purchase(self, iid, pid, items_removed):
    if self._is_item_final(iid) and not self._is_item_upgrade(iid):
      return iid
    else:
      return None

  def get_builds_from_match(self, match):
    builds = {}
    items_removed = {}
    items_upgraded = {}
    items_undone = {}
    for p in match["participants"]:
      pindex = p["participantId"] - 1
      p["runes"].sort(key=lambda x: x["runeId"])
      p["masteries"].sort(key=lambda x: x["masteryId"])
      builds[p["participantId"]] = {
        # "summonerId": match["participantIdentities"][pindex]["player"]["summonerId"],
        # "teamId": p["teamId"],
        # "highestAchievedSeasonTier": p["highestAchievedSeasonTier"],
        # "timeline": p["timeline"],
        "stats": p["stats"],
        # "rawItemEvents": [],  # from timeline
        "build": {
          "championId": p["championId"],
          "lane": p["timeline"]["lane"],
          "role": p["timeline"]["role"],
          "skillups": [], # from timeline
          "summonerSpells": [p["spell1Id"], p["spell2Id"]],
          "runes": p["runes"],
          "masteries": p["masteries"],
          "itemEvents": [], # from timeline
          "finalBuild": [], # from timeline
        }
      }
      items_removed[p["participantId"]] = {}
      items_undone[p["participantId"]] = 0

    for frame in reversed(match["timeline"]["frames"]):
      if not "events" in frame:
        continue

      for event in reversed(frame["events"]):
        eventType = event["eventType"]
        pid = event["participantId"] if ("participantId" in event) else None
        if pid < 1 or pid > 10: continue
        build = builds[pid]["build"]

        # append history if related to items
        if "itemId" in event:
          event["itemId"] = str(event["itemId"]).zfill(4)
        # if "ITEM" in eventType: builds[pid]["rawItemEvents"].append(event)

        if eventType == "SKILL_LEVEL_UP" and event["levelUpType"] == "NORMAL":
          build["skillups"].append(str(event["skillSlot"]))
          continue
        elif eventType == "ITEM_SOLD":
          if items_undone[pid] > 0:
            items_undone[pid] -= 1
            continue

          iid = event["itemId"]
          if self._is_item_upgrade(iid):  # in case they sold an upgrade
            self._register_item_upgrade_sold(iid, pid, items_removed)
          self._update_items_removed(iid, pid, items_removed)
        elif eventType == "ITEM_PURCHASED":
          if items_undone[pid] > 0:
            items_undone[pid] -= 1
            continue

          iid = event["itemId"]
          removed = items_removed[pid]
          if iid in removed and removed[iid] > 0:
            removed[iid] -= 1
            continue

          item = self._process_item_purchase(iid, pid, items_removed)
          if item is not None:
            event["is_final_item"] = True
            build["finalBuild"].append(item)

          # Append event if not undone or removed (sold)
          trimmed_event = {
              "itemId": event["itemId"],
              "timestamp": event["timestamp"]
          }
          if "is_final_item" in event: trimmed_event["is_final_item"] = event["is_final_item"]
          if not self._itemDb.is_potion_or_trinket(iid):
            build["itemEvents"].append(trimmed_event)
        elif eventType == "ITEM_UNDO":
          items_undone[pid] += 1
        else:
          continue

    # Correct reversed orderings and return builds as list
    ret = []
    for pid in builds:
      build = builds[pid]["build"]
      # builds[pid]["rawItemEvents"].reverse()
      build["itemEvents"].reverse()
      build["skillups"].reverse()
      build["finalBuild"].reverse()
      ret.append(builds[pid])

      if len(build["finalBuild"]) > 6:
        print "!! BUILD LEN > 6: %r, %r" % (match["matchId"], build["finalBuild"])

    return ret




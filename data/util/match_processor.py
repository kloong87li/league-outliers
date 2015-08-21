
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
      "participantStats": [],  # list of database ids
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

  def _process_item_purchase(self, iid, pid, items_removed, items_upgraded):
    upgraded = items_upgraded[pid]
    if iid in upgraded and upgraded[iid] > 0:
      upgraded[iid] -= 1
      return None

    if self._is_item_upgrade(iid):
      item = self._itemDb.get_item(iid)
      if item and "from" in item:
        for base_iid in item["from"]:
          self._update_items_removed(base_iid, pid, items_upgraded)
      return iid
    elif self._is_item_final(iid):
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
      builds[p["participantId"]] = {
        "summonerId": match["participantIdentities"][pindex]["player"]["summonerId"],
        "rawItemEvents": [],
        "itemEvents": [],
        "skillUps": [],
        "finalBuild": [],
        "participant": p 
      }
      items_removed[p["participantId"]] = {}
      items_upgraded[p["participantId"]] = {}
      items_undone[p["participantId"]] = 0

    for frame in reversed(match["timeline"]["frames"]):
      if not "events" in frame:
        continue

      for event in reversed(frame["events"]):
        eventType = event["eventType"]
        pid = event["participantId"] if ("participantId" in event) else None
        if pid < 1 or pid > 10: continue

        # append history if related to items
        if "itemId" in event:
          event["itemId"] = str(event["itemId"])
        if "ITEM" in eventType: builds[pid]["rawItemEvents"].append(event)

        if eventType == "SKILL_LEVEL_UP":
          builds[pid]["skillUps"].append({
            "skillSlot": event["skillSlot"],
            "levelUpType": event["levelUpType"]
          })
          continue
        elif eventType == "ITEM_SOLD":
          if items_undone[pid] > 0:
            items_undone[pid] -= 1
            continue
          iid = event["itemId"]
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

          item = self._process_item_purchase(iid, pid, items_removed, items_upgraded)
          if item is not None:
            event["is_final_item"] = True
            builds[pid]["finalBuild"].append(item)

          # Append event if not undone or removed (sold)
          builds[pid]["itemEvents"].append(event)
        elif eventType == "ITEM_UNDO":
          items_undone[pid] += 1
        else:
          continue

    # Correct reversed orderings and return builds as list
    ret = []
    for pid in builds:
      builds[pid]["rawItemEvents"].reverse()
      builds[pid]["itemEvents"].reverse()
      builds[pid]["skillUps"].reverse()
      builds[pid]["finalBuild"].reverse()
      builds[pid]["matchId"] = match["matchId"]
      ret.append(builds[pid])

    return ret




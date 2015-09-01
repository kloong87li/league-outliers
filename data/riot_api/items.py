from .api import RiotApi

class RiotItems(object):

  # For certain items to be considered final, despite being upgradeable
  _FINAL_ITEM_WHITELIST = [
    2049, # sightstone
    3117, 3009, 3006, 3158, 3047, 3020, 3111, # boots
    3003, # archangels
    3004, # muramane
    3710, # devourers
    3718,
    3722,
    3726
  ]

  # items not considered as final, trinkets and stuff
  _FINAL_ITEM_BLACKLIST = [
    3364,
    3363,
    3362,
    3361,
    2004,
    2003,
    2044,
    2043,
    2140,
    2139,
    2138,
    2137,
    2009,
    2010,
    1055,
    1054,
    1056,
    3599, # kalista spear thing
    3340,
    3341,
    3342,
    3361,
    3362,
    3363,
    3364
  ]

  _GROUP_BLACKLIST = [
    "GangplankRUpgrade",
    "Boots"
  ]

  def __init__(self):
    try:
      self._items = RiotApi.get_all_items()["data"]
    except Exception as e:
      print "!! [ITEMS] Exception: %r" % e

    for iid in RiotItems._FINAL_ITEM_BLACKLIST:
      iid = str(iid)
      self._items[iid]["into"] = []

    for iid in RiotItems._FINAL_ITEM_WHITELIST:
      iid = str(iid)
      upgrade = self._items[iid]["into"]
      del self._items[iid]["into"]
      self._items[iid]["upgradeInto"] = upgrade

  def get_item(self, iid):
    if iid in self._items:
      return self._items[iid]
    else:
      return None

  def is_final_item(self, iid):
    item = self.get_item(iid)
    if item is None:
      return False

    # Avoid gangplank items
    if "group" in item:
      for group in RiotItems._GROUP_BLACKLIST:
        if group in item["group"]:
          return False

    return ("into" not in item)

  def is_upgraded_item(self, iid):
    item = self.get_item(iid)
    if item is None:
      return False

    fromItems = item["from"] if "from" in item else None
    if fromItems is not None:
      for from_iid in fromItems:
        if self.is_final_item(from_iid): return True

    return False

  def is_item_upgradeable(self, iid):
    # Mainly used to avoid considering boot upgrades
    item = self.get_item(iid)
    if item is None:
      return False

    if "upgradeInto" in item:
      for intoItem in item["upgradeInto"]:
        if self._is_in_group_blacklist(intoItem):
          return False
      return True
    else:
      return False

  def _is_in_group_blacklist(self, iid):
    item = self.get_item(iid)
    if "group" in item:
      for group in RiotItems._GROUP_BLACKLIST:
        if group in item["group"]:
          return True
    return False

  def is_potion_or_trinket(self, iid):
    return int(iid) in RiotItems._FINAL_ITEM_BLACKLIST


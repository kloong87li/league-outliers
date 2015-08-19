
class MatchDb(object):

  def __init__(self, match_collection):
    self._matches = match_collection
    return

  def insert(self, match):
    print "[MATCHES] Inserted match %s" % match["matchId"]
    self._matches.insert_one(match)

  def contains(self, match_ref):
    return bool(self._matches.find_one({"matchId": match_ref["matchId"]}))

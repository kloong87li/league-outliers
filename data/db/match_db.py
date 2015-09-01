
class MatchDb(object):

  def __init__(self, match_collection):
    self._matches = match_collection
    self._matches.create_index("matchId")
    return

  def insert_ref(self, match_ref):
    match_ref.update({"is_ref": True})
    self._matches.insert(match_ref)

  def mark(self, match):
    match.update({"is_ref": False})
    self._matches.replace_one(
      {"matchId": match["matchId"]},
      match,
      upsert=True
    )

  def contains(self, match_ref):
    return bool(self._matches.find_one({"matchId": match_ref["matchId"]}))

  def find_needs_update(self):
    return self._matches.find_one_and_update(
      {"is_ref": True},
      {"$set": {"is_ref": False}}
    )

  def return_match(self, match_ref):
    self._matches.update(
      {"matchId": match_ref["matchId"]},
      {"$set": {"is_ref": True}}
    )

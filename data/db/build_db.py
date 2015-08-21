
class BuildDb(object):

  def __init__(self, player_builds):
    self._player_builds = player_builds
    return

  def insert_many(self, builds):
    result = self._player_builds.insert_many(builds)
    return result.inserted_ids

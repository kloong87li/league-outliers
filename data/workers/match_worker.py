import functools
from .worker import Worker 
from riot_api import RiotApi, ApiRequest
from Queue import Full, Empty

class MatchWorker(Worker):

  def __init__(self, **kwargs):
    super(MatchWorker, self).__init__(name="MATCH_WORKER")

    self._last_update = kwargs.pop("last_update")
    self._api_scheduler = kwargs.pop("api_scheduler")
    self._player_db = kwargs.pop("player_db")
    self._match_db = kwargs.pop("match_db")
    self._build_db = kwargs.pop("build_db")
    self._player_queue = kwargs.pop("player_queue") # Player
    self._match_queue = kwargs.pop("match_queue")  # MatchReference
    self._match_processor = kwargs.pop("match_processor")

  def _generate_request(self, match_ref):
    get = functools.partial(RiotApi.get_match, match_ref["matchId"])
    return ApiRequest(get)

  def _player_is_good(self, league):
    return (league in ["CHALLENGER", "MASTER", "DIAMOND", "PLATINUM", "GOLD"])

  def _queue_players(self, players, leagues):
    for i in xrange(len(players)):
      player = players[i]
      try:
        # Verify that player is good
        player["league"] = leagues[i]
        if not self._player_is_good(player["league"]):
          return

        player = self._player_db.find_or_create(player)
        if player["last_update"] < self._last_update:
          self._player_queue.put(player, False)
      except Full:
        return

  def _get_next_match(self):
    match = None
    try:
      # TODO add option to update old
      match = self._match_queue.get(True, Worker._QUEUE_TIMEOUT) or self._match_db.find_needs_update()
      return match
    except Empty:
      return self._match_db.find_needs_update()

  def _process_and_insert_build(self, participant):
    build = participant["build"]

    build["runes"] = self._build_db.insert_runes(build["runes"])
    build["masteries"] = self._build_db.insert_masteries(build["masteries"])
    build["skillups"] = self._build_db.insert_skillups(build["skillups"])

    return self._build_db.insert_build(participant)

  def _make_request(self, request):
    # Try to queue api request
    while self._is_running:
      try:
        self._api_scheduler.add_request(request, Worker._SCHEDULER_TIMEOUT)
        break
      except Full:
        continue

    # Wait for response
    while self._is_running:
      if request.wait(Worker._API_REQUEST_TIMEOUT):
        break

  def _perform_work(self):
    # Get next match from queue and generate request    
    match_ref = self._get_next_match()
    if match_ref is None: return
    request = self._generate_request(match_ref)

    self._make_request(request)

    match = request.get_data()
    if match is None:
      self._match_db.return_match(match_ref)
      print "!! [MATCH_WORKER] Failed to get match, reseting 'is_ref' for %r" % match_ref["matchId"]
      return
    
    # Insert players
    players = [p["player"] for p in match["participantIdentities"]]
    leagues = [p["highestAchievedSeasonTier"] for p in match["participants"]]
    self._queue_players(players, leagues)

    # Process and insert match
    try:
      trimmed = self._match_processor.trim_match(match)
      participants = self._match_processor.get_builds_from_match(match)
      
      # Consolidate build, runes, masteries, etc into separate db's
      for p in participants:
        build_id = self._process_and_insert_build(p)
        p["build"] = build_id

      trimmed["participants"] = participants
      self._match_db.insert(trimmed)
      print "[MATCH_WORKER] Inserted match %r" % match["matchId"]
    except Exception as e:
      print "!! Exception occurred for match: %d (%r)" % (match["matchId"], e)
      import traceback
      traceback.print_exc()
      return








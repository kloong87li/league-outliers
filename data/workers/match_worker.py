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
    self._player_queue = kwargs.pop("player_queue") # Player
    self._match_queue = kwargs.pop("match_queue")  # MatchReference

  def _generate_request(self, match_ref):
    get = functools.partial(RiotApi.get_match, match_ref["matchId"])
    return ApiRequest(get)


  def _get_leagues_for_players(self, players):
    get = functools.partial(RiotApi.get_leagues, players)
    request = ApiRequest(get)

    self._make_request(request)

    leagues = request.get_data()
    return leagues or {}

  def _get_league(self, league_map, player):
    sid = str(player["summonerId"])
    try:
      leagues = league_map[sid]
      for league in leagues:
        if league["queue"] == "RANKED_SOLO_5x5":
          return league["tier"]
      return None
    except KeyError:
      return None

  def _player_is_good(self, league):
    return (league in ["CHALLENGER", "MASTER", "DIAMOND", "PLATINUM", "GOLD"])

  def _queue_players(self, players):
    league_map = self._get_leagues_for_players(players)

    for player in players:
      try:
        # Verify that player is good
        player["league"] = self._get_league(league_map, player)
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
      return None

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
    
    # Update db and queue
    print "[MATCH_WORKER] Inserted match %r" % match["matchId"]
    self._match_db.insert(match)
    players = [p["player"] for p in match["participantIdentities"]]
    self._queue_players(players)








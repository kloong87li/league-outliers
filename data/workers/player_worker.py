import functools
from .worker import Worker 
from riot_api import RiotApi, ApiRequest
from Queue import Full, Empty

class PlayerWorker(Worker):

  def __init__(self, **kwargs):
    super(PlayerWorker, self).__init__(name="PLAYER_WORKER")

    # Whether to prioritze updating old records or collecting new
    self._is_prioritize_new = kwargs.pop("is_prioritize_new")

    self._api_scheduler = kwargs.pop("api_scheduler")
    self._player_db = kwargs.pop("player_db")
    self._match_db = kwargs.pop("match_db")
    self._last_update = kwargs.pop("last_update")
    self._player_queue = kwargs.pop("player_queue") # Player
    self._match_queue = kwargs.pop("match_queue")  # MatchReference

  def _generate_request(self, player):
    get = functools.partial(RiotApi.get_matches, player["summonerId"], {
        "rankedQueues": "RANKED_SOLO_5x5",  # TODO include ranked 5's?
        "beginTime": player["last_update"]
    })

    return ApiRequest(get)

  def _get_player_from_queue(self):
    try:
      player = self._player_queue.get(True, Worker._QUEUE_TIMEOUT)
      return player
    except Empty:
      return None

  def _get_next_player(self):
    player = None
    if self._is_prioritize_new:
      player = self._get_player_from_queue() or self._player_db.find_stale(self._last_update)
    else:
      player = self._player_db.find_stale(self._last_update) or self._get_player_from_queue()
    return player

  def _queue_matches(self, matches):
    for match_ref in matches:
      if not self._match_db.contains(match_ref):
        self._match_db.insert_ref(match_ref)
        self._match_queue.put(match_ref)

  def _perform_work(self):
    # Generate request
    player = self._get_next_player()
    if player is None: return
    request = self._generate_request(player)
    
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

    data = request.get_data()
    if data is None:
      # TODO, reset player if request didnt finish
      print "!! [PLAYER_WORKER] Failed to get player: %s, returning to DB" % player["summonerName"]
      self._player_db.return_player(player)
      return

    # Update db and queue
    print ("[PLAYER_WORKER] Updated player %s with time %r (last update was %r)" %
      (player["summonerName"], request.get_timestamp(), player["last_update"]))
    self._player_db.update_matches(player, request.get_timestamp())
    if data["totalGames"] > 0:
      self._queue_matches(data["matches"])








import threading


class PlayerWorker():

  def __init__(self, **kwargs):
    self._api = kwargs.pop("api")
    self._player_db = kwargs.pop("player_db")
    self._last_update = kwargs.pop("last_update")
    self._player_queue = kwargs.pop("player_queue")
    self._match_queue = kwarsgs.pop("match_queue")

    self._is_running = False
    self._thread = threading.Thread(target=self._run, name="PLAYER WORKER")

  def _run(self):
    


  def start(self):
    self._is_running = True
    self._thread.start()

  def stop(self):
    self._is_running = False
    self._thread.join()
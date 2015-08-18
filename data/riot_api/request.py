import threading

class ApiRequest():

  def __init__(self):
    self._done_event = threading.Event()
    self._data = None

  def execute(self, api_fn):
    # Could raise RiotApiException or RiotRateLimitException
    self._data = api_fn()
    self._done_event.set()

  def wait(self, timeout=None):
    return self._done_event.wait(timeout)

  def get_data(self):
    return self._data
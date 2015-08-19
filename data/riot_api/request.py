import threading
import time

class ApiRequest(object):

  def __init__(self, api_fn):
    self._api_fn = api_fn

    self._done_event = threading.Event()
    self._done_event.clear()
    self._data = None
    self._timestamp = None

  def execute(self):
    # Could raise RiotApiException or RiotRateLimitException
    if not self._done_event.is_set():
      self._timestamp = int(time.time() * 1000)
      self._data = self._api_fn()
      self._done_event.set()

  def mark_invalid(self):
    self._data = None
    self._done_event.set()

  def wait(self, timeout=None):
    return self._done_event.wait(timeout)

  def get_data(self):
    return self._data

  def get_timestamp(self):
    return self._timestamp
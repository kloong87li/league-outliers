import threading
import functools
from Queue import Queue, Empty, Full
from concurrent.futures import ThreadPoolExecutor

from .api import RiotApiException, RiotRateLimitException

class RiotApiScheduler():
  MAX_QSIZE = 20
  REQ_PER_MIN = 50

  def __init__(self):
    self._timer = threading.Timer(0, self._run)
    self._lock = threading.Lock()
    self._executor = ThreadPoolExecutor(max_workers=5)
    self._queue = Queue(maxsize=RiotApiScheduler.MAX_QSIZE)

    self._backoff = None  # guarded by lock
    self._next_sleep = 60.0 / RiotApiScheduler.REQ_PER_MIN  # initial sleep period

  def _process_request(self, req):
    try:
      req.execute()
    except RiotRateLimitException as e:
      print "[API Rate Limit] Retry after: %d" % e.retry_after
      with self._lock:
        self._backoff = e.retry_after
    except RiotApiException as e:
      print "[API Exception] Code: %d" % e.status_code

  def _run(self):
    # TODO optimize to take better advantage of api limit
    # Pull requests off per rate limit and schedule them with thread pool executor
    try:
      req = self._queue.get(False)
      self._executor.submit(self._process_request, req)
    except Empty:
      pass

    if self._is_running:
      delay = self._next_sleep
      with self._lock:
        if self._backoff:
          delay = self._backoff
          self._backoff = None

      self._timer = threading.Timer(delay, self._run)
      self._timer.start()
    return

  def add_request(self, req, timeout=1):
    # should block if full, raises Full exception if times out
    while self._is_running:
      try:
        self._queue.put(req, True, timeout)
        return
      except Full:
        continue

  def start(self):
    self._is_running = True
    self._timer.start()

  def stop(self):
    self._is_running = False
    self._executor.shutdown(wait=True)
    self._timer.cancel()

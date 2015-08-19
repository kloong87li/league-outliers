import threading

class Worker(object):
  _SCHEDULER_TIMEOUT = .3  # seconds
  _API_REQUEST_TIMEOUT = .3  # seconds
  _QUEUE_TIMEOUT = .3  # seconds

  def __init__(self, **kwargs):
    self._name = kwargs.pop("name", "WORKER")
    self._is_running = False
    self._thread = threading.Thread(target=self._run, name=self._name)

  def _perform_work(self):
    # Override this
    pass

  def _run(self):
    while self._is_running:
      self._perform_work()

  def start(self):
    self._is_running = True
    self._thread.start()

  def stop(self):
    self._is_running = False
    self._thread.join()
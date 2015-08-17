from riot_api import RiotApiScheduler, ApiRequest
import signal
import sys

if __name__ == "__main__":

  # Register stop signal handler
  def stop(sig, frame):
    x.stop()
  signal.signal(signal.SIGINT, stop)

  
  
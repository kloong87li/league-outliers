import datetime
from .match_processor import MatchProcessor

def datetime_to_timestamp(dt):
  return int((dt - datetime.datetime(1970,1,1)).total_seconds() * 1000)
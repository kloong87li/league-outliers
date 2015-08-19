import datetime

def datetime_to_timestamp(dt):
  return int((dt - datetime.datetime(1970,1,1)).total_seconds() * 1000)
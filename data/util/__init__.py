def datetime_to_timestamp(datetime):
  return int((datetime - datetime.datetime(1970,1,1)).total_seconds() * 1000)
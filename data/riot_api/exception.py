class RiotApiException(Exception):
  def __init__(self, status_code, info, url):
    super(RiotApiException, self).__init__()
    self.status_code = status_code
    self.info = info
    self.url = url

  def __str__(self):
    return "Riot API returned status code: %r for url %s" % (str(self.status_code), self.url)

class RiotRateLimitException(Exception):
  def __init__(self, limit_type, retry_after):
    super(RiotRateLimitException, self).__init__()
    self.retry_after = retry_after
    self.limit_type = limit_type

  def __str__(self):
    return "Riot API indicated rate limit reached, retry after: %r" % str(self.retry_after)
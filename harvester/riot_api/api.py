import urllib, json
from .api_key import API_KEY

class RiotApiException(Exception):
  def __init__(self, status_code, info):
    self.status_code = status_code
    self.info = info

  def __str__():
    return "Riot API returned status code: %d" % self.status_code

class RiotRateLimitException(Exception):
  def __init__(self, limi_type, retry_after):
    self.retry_after = retry_after
    self.limi_type = limit_type

  def __str__():
    return "Riot API indicated rate limit reached, retry after: %d" % self.retry_after

class RiotApi():

  _FORMAT_URL = "https://na.api.pvp.net/api/lol/{region}/v{version}/{endpoint}?{query_params}"
  _REGION = "na"

  @staticmethod
  def _get(full_url):
    # Raises RiotApiException
    resp = urllib.urlopen(full_url)
    status = resp.getcode()
    if status == 200:
      data = json.loads(resp.read())
      resp.close()
      return data
    elif status == 429:
      info = resp.info()
      limit_type = info.getheader("X-Rate-Limit-Type")
      retry_after = info.getheader("Retry-After")
      raise RiotRateLimitException(limit_type, retry_after)
    else:
      raise RiotApiException(status_code, resp.info())

  @staticmethod
  def _get_api_url(endpoint, version, query_params=None):
    if query_params is None:
      query_params = {}

    query_params.update({"api_key": API_KEY})
    full_url = RiotApi._FORMAT_URL.format(
      region=RiotApi._REGION,
      version=version,
      endpoint=endpoint,
      query_params=urllib.urlencode(query_params)
    )
    return full_url

  ###
  # Public Methods
  #
  ###

  @staticmethod
  def get_match(match_id, includeTimeline=True):
    params = {
    "includeTimeline": includeTimeline
    }
    url = RiotApi._get_api_url("match/%s" % match_id, 1.4, params)
    return RiotApi._get(url)

  @staticmethod
  def get_matches(summoner_id, query_params=None):
    url = RiotApi._get_api_url("matchlist/by-summoner/%s" % summoner_id, 2.2, query_params)
    return RiotApi._get(url)








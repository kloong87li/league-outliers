import urllib, json
from .api_key import API_KEY
from .exception import RiotRateLimitException, RiotApiException

class RiotApi(object):
  _API_KEY = API_KEY
  _FORMAT_URL = "https://na.api.pvp.net/api/lol/{region}/v{version}/{endpoint}?{query_params}"
  _REGION = "na"
  _DEFAULT_BACKOFF = 5

  @staticmethod
  def set_api_key(key):
    RiotApi._API_KEY = key

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
      retry_after = info.getheader("Retry-After") or RiotApi._DEFAULT_BACKOFF
      raise RiotRateLimitException(limit_type, float(retry_after))
    else:
      raise RiotApiException(status, resp.info(), full_url)

  @staticmethod
  def _get_api_url(endpoint, version, query_params=None):
    if query_params is None:
      query_params = {}

    query_params.update({"api_key": RiotApi._API_KEY})
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
    url = RiotApi._get_api_url("match/%s" % match_id, 2.2, params)
    return RiotApi._get(url)

  @staticmethod
  def get_matches(summoner_id, query_params=None):
    url = RiotApi._get_api_url("matchlist/by-summoner/%s" % summoner_id, 2.2, query_params)
    return RiotApi._get(url)

  @staticmethod
  def get_leagues(players):
    summoner_ids = ",".join([str(p["summonerId"]) for p in players])
    url = RiotApi._get_api_url("league/by-summoner/%s" % summoner_ids, 2.5)
    return RiotApi._get(url)

  @staticmethod
  def get_all_items():
    url = "https://na.api.pvp.net/api/lol/static-data/na/v1.2/item?api_key={api_key}&itemListData=into,from".format(
      api_key=RiotApi._API_KEY
    )
    return _get(url)





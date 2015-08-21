from flask import (
  Flask,
  request,
  session, g, redirect,
  url_for,
  abort, render_template, flash
)
from flask_restful import Resource, Api

from pymongo import MongoClient
from bson import json_util
import json

SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'
DEBUG = True


app = Flask(__name__)
app.config.from_object(__name__)
api = Api(app)
mongo = MongoClient("mongodb://localhost:27017")

# Enable CORS
@app.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Credentials', 'true')
  response.headers.add('Access-Control-Allow-Headers', 'Origin, Content-Type, Accept, Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET, PUT, POST, DELETE, OPTIONS')
  return response


def toJson(data):
  """Convert Mongo object(s) to JSON"""
  return json.dumps(data, default=json_util.default)

class CommonBuilds(Resource):
  def get(self, id):
    results = mongo.outliers.demo_build_stats.find({
      "$query": {"_id.championId": int(id)},
      "$orderby": {"value.stats.count": 1}
    })
    json_results = []
    for result in results:
      print result
      json_results.append(result)
    return {"data": json.loads(toJson(json_results))}, 200

class OutlierBuilds(Resource):
  def get(self, id):
    return id, 200


api.add_resource(CommonBuilds, '/api/champion/common/<string:id>')
api.add_resource(OutlierBuilds, '/api/champion/outlier/<string:id>')

if __name__ == '__main__':
    app.run(host='0.0.0.0')
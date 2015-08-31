# Outlier.gg Backend

Outlier.gg is a site for finding off-meta builds for League of Legends. See [http://outlier.gg/](http://outlier.gg/)

### Tech Stack

* Python for data collection scripts
* MongoDb (PyMongo driver) for data storage
* Python Flask for web API
* AWS (EC2 micro isntance and EBS storage)

### Program Structure

There are 3 components to the Outlier.gg backend.

1. Collection (collect.py)
2. Consolidation (consolidate.py)
3. Aggregation (finalize.py)

Each of these steps can be started via a python script (uses the argparser module, so feel free to use -h for detailed options).

There is also a web server component for serving data to the frontend (`web/main.py`)

##### Data collection

For data collection, we run two types of threads (match workers and player workers). Both submit requests to a scheduler (`riot_api/scheduler`) that guarantees we don't exceed the API rate limit.

Match workers fetch matches from the API and place them in a player queue, while the player workers fetch players' match history and place matches in the match queue.

Each build we encounter gets placed in a database, where its stats are combined into the existing stats we have for that build. Therefore the longer the data collection step is run, the more accurate the results are.

##### Consolidation

We use MongoDB's aggregation framework for data analytics. We run a series of aggregations and map-reduces to group builds into their "final builds" along with corresponding runes, masteries, and item sets. See [http://outlier.gg/about](http://outlier.gg/about) for more detailed methodology.

##### Aggregation/Finalization

In the final step, we run more map-reduce tasks to group builds by champion and determine a set of "unique builds" that we can then serve on the site. See [http://outlier.gg/about](http://outlier.gg/about) for more detailed methodology.

##### Future extensions

Due to time and resource constraints, there were a few things we weren't able to accomplish, but were originally planned:

1. Ongoing collection + aggregation. Ideally we have background jobs collecting and aggregating data on a regular basis, however right now we simply have scripts that we need to manually start and stop. Making this change shouldn't be too difficult, but would require more AWS resources (currently using the free tier running on a micro ec2 instance)
2. Recommendations for summoners. We originally wanted to generate potential "outlier" builds for a given summoner to try out, but we didn't have time to implement this.
3. Determining trends (i.e build X has increased in popularity by 5%)
4. Better handling of large datasets. Right now the aggregation and collection process is not as efficient as we'd like it to be. (This could also be improved by pouring more money into AWS)

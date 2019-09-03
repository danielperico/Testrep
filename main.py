from store_to_database import datetime_from_utc_to_local
from store_to_database import save_tweets_to_db
from store_to_database import find_max_id
from store_to_database import retrieve_geocodes_from_db
from store_to_database import find_geocode_id
# from store_to_database import find_min_id_api
from store_to_database import find_min_id_db
import credentials
import tweepy
from tweepy import OAuthHandler
import time
import sqlite3

start_time = time.time()

# Necessary steps for tweepy authorisation
# PersonalCredentials
consumer_key = credentials.get_consumer_key()
consumer_secret = credentials.get_consumer_secret()
access_token = credentials.get_access_token()
access_secret = credentials.get_access_secret()
# Authorisation
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Connect to database
database_file_name = "C:\\Users\Paul\Desktop\h.sqlite"

# Variables
query = "*"
geocodes = retrieve_geocodes_from_db(database_file_name)

# Find min and max id
# mind id either from api:
# min_id, min_id_timestamp = find_min_id_api(api=api, query=query, geocode=geocodes[0], days_back=2)
# or from database
min_id = find_min_id_db(database_file_name=database_file_name) + 1
print("Min ID: " + str(min_id))

max_id = find_max_id(api=api, query=query)
print("Max ID: " + str(max_id))

# Import Tweets
tmp_id = []
tmp_timestamp = []
for geocode in geocodes:
    bounding_box_id = find_geocode_id(geocode=geocode, database_file_name=database_file_name)
    max_id_tmp = max_id
    while max_id_tmp > min_id:
        try:
            conn = sqlite3.connect(database_file_name)
            c = conn.cursor()
            for tweet in tweepy.Cursor(api.search, q=query, geocode=geocode, min_id=min_id,
                                       max_id=max_id_tmp).items(100):
                tmp_id.append(tweet.id)
                tmp_timestamp.append(tweet.created_at)
                save_tweets_to_db(tweet=tweet, c=c, bounding_box_id=bounding_box_id)
            new_max_id = min(tmp_id)
            max_id_tmp = new_max_id - 1  # make the id of oldest tweet of the 100 obtained the max for next batch
            print(str(len(tmp_id)) + ' tweets added for geocode ' + str(bounding_box_id) + ': ' + str(geocode)
                  + '. Max ID = ' + str(max_id_tmp) + '. Min ID = ' + str(min_id) + '. Tweets were made between: '
                  + str(datetime_from_utc_to_local(min(tmp_timestamp))) + ' and '
                  + str(datetime_from_utc_to_local(max(tmp_timestamp))))
            tmp_id = []
            tmp_timestamp = []
            conn.commit()
            conn.close()
        except ValueError:
            print('No more tweets, continue...')
            max_id_tmp = max_id  # set max_id_tmp back to max_id
            break  # go to next geocode
        except tweepy.error.TweepError as e:
            if e.api_code == 500:
                timeout = 1000
                print("Twitter error response, status code = 500. Wait for 15 min.")
                continue
            elif e.api_code == 503:
                timeout = 1000
                print("Twitter error response, status code = 503. Wait for 15 min.")
                continue
            else:
                print(e.reason)
                break

print("--- %s seconds ---" % (time.time() - start_time))

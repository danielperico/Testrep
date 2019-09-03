import json
import random
import re
import sqlite3
from re import error

import pandas as pd
import tweepy
from aiohttp.client_exceptions import ServerDisconnectedError
from tweepy import TweepError

import credentials
import queries
import twint
from store_to_database import save_user_to_db

# https://github.com/twintproject/twint

table_name = 'tweets'
# TODO define path to the database file
sqlite_file = credentials.get_db_path()
# TODO set the ID segment you're going to cover: 0 Paun, 1 Andre, 2 Paul
segment_id = credentials.get_segment_id()


def authenticate_tweepy():
    auth = tweepy.OAuthHandler(credentials.get_consumer_key(), credentials.get_consumer_secret())
    auth.set_access_token(credentials.get_access_token(), credentials.get_access_secret())
    global twitter
    twitter = tweepy.API(auth)


def create_database_connection() -> bool:
    try:
        global conn
        conn = sqlite3.connect(sqlite_file)
        return True
    except sqlite3.Error as e:
        print(e)
        return False


def get_location_bounding_box():
    return list(pd.read_sql_query("SELECT bounding_box_id, city_name FROM bounding_box", conn).itertuples(index=False,
                                                                                                          name=None))


def entry_exists(table: str, target_column: str, column_id: str, id_value) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT " + target_column + " FROM " + table + " WHERE " + column_id + " = ?", [id_value])
    data = cur.fetchone()
    if data is None:
        return False
    elif not data[0]:
        return False
    else:
        return True


def get_data(table: str, column_id: str, id_value):
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM " + table + " WHERE " + column_id + " = ?", [id_value])
    return cur.fetchone()


def get_users():
    users_table = pd.read_sql_query("SELECT screen_name, location, id FROM users WHERE is_history_complete=0", conn)
    return users_table['screen_name'].tolist(), users_table['location'].tolist(), users_table['id'].tolist()


def scrape_user_profile(box_id: str, username: str, limit=None, last_tweet_id=None):
    print('log: parsing tweets from user: ' + username)
    c = twint.Config()
    c.Username = username
    c.Limit = limit
    c.Connection = conn
    c.Homebrewed_database = True
    c.Box_id = box_id
    c.Twitter_api = twitter
    c.Last_tweet_id = last_tweet_id
    # c.Since = '2018-01-01'
    c.Timedelta = 30
    twint.run.Profile(c)


def insert_tweet_into_db(entry, config):
    global conn
    conn = config.Connection
    print('log: \tuser: ' + config.Username + ', tweet-id: ' + entry.id_str)
    if not entry_exists('tweets', 'id', 'id', entry.id):
        insert_data('tweets', ('created_at', 'id', 'id_str', 'text', 'user_id', 'reply_count', 'retweet_count',
                               'favorite_count', 'is_ext_seach', 'bounding_box_id'),
                    (entry.datestamp + ' ' + entry.timestamp, entry.id, entry.id_str, entry.tweet, entry.user_id,
                     entry.replies_count, entry.retweets_count, entry.likes_count, 1, config.Box_id))
        if entry.hashtags:
            for tag in entry.hashtags:
                pos = get_pattern_position(tag, entry.tweet)
                insert_data('hashtag', ('tweet_id', 'index_start', 'index_end', 'text'),
                            (entry.id, *pos, tag))

        if entry.urls:
            for url in entry.urls:
                pos = get_pattern_position(url, entry.tweet)
                insert_data('url', ('tweet_id', 'expanded_url', 'indices_start', 'indices_end', 'url'),
                            (entry.id, url, *pos, url))

        if entry.mentions:
            for mention in entry.mentions:
                if entry_exists('users', 'screen_name', 'screen_name', mention):
                    user = get_data('users', 'screen_name', mention)
                    user_id = user[0]
                    user_name = user[1]
                else:
                    try:
                        # insert new user
                        user = config.Twitter_api.get_user(screen_name=mention)
                        save_user_to_db(conn.cursor(), user)
                        insert_data('users', (
                            'id', 'id_str', 'name', 'screen_name', 'location', 'url', 'description', 'is_verified',
                            'followers_count', 'friends_count', 'listed_count', 'favourites_count',
                            'statuses_count',
                            'created_at', 'is_geo_enabled', 'lang', 'is_history_complete'),
                                    (user.id, user.id_str, user.name, user.screen_name, user.location, user.url,
                                     user.description,
                                     user.verified, user.followers_count, user.friends_count, user.listed_count,
                                     user.favourites_count,
                                     user.statuses_count, user.created_at, user.geo_enabled, user.lang, False))
                        user_id = user.id
                        user_name = user.name
                        conn.commit()
                    except TweepError:
                        user_id = -1
                        user_name = ''

                # insert @
                pos = get_pattern_position(mention, entry.tweet)
                insert_data('user_mention', (
                    'tweet_id', 'user_mention_id', 'user_mention_id_str', 'indices_start', 'indices_end', 'name',
                    'screen_name'), (entry.id, user_id, str(user_id), *pos, user_name, mention))

        if entry.photos:
            for pic in entry.photos:
                pos = get_pattern_position(pic, entry.tweet, is_picture=True)
                display_url = entry.tweet[pos[0]:pos[1]]
                insert_data('media', (
                    'tweet_id', 'display_url', 'media_id', 'indices_start', 'indices_end', 'media_url', 'type'),
                            (entry.id, display_url, -1, *pos, pic, 'photo'))


def get_pattern_position(pattern: str, text: str, is_picture=False):
    try:
        if is_picture:
            return re.search(r'pic\.twitter\.com/\S+', text.lower()).span(0)
        return re.search(pattern, text.lower()).span(0)
    except (AttributeError, error):
        return -1, -1


def insert_json_into_db(box_id, username: str):
    print('log: parsing tweets from user: ' + username)
    with open(('tweets/' + username + '/' + 'tweets.json'), 'r', encoding='utf-16') as data_file:
        for line in data_file.readlines():
            entry = json.loads(''.join([line]))
            print('log: \tuser: ' + username + ', tweet-id: ' + str(entry['id']))
            insert_data('tweets', ('created_at', 'id', 'id_str', 'text', 'user_id', 'reply_count', 'retweet_count',
                                   'favorite_count', 'is_ext_seach', 'bounding_box_id'),
                        (entry['date'] + ' ' + entry['time'], entry['id'], str(entry['id']), entry['tweet'],
                         entry['user_id'], entry['replies_count'], entry['retweets_count'], entry['likes_count'], 1,
                         box_id))
            if entry['hashtags']:
                for tag in entry['hashtags']:
                    pos = get_pattern_position(tag, entry['tweet'])
                    insert_data('hashtag', ('tweet_id', 'index_start', 'index_end', 'text'),
                                (entry['id'], *pos, tag))

            if entry['urls']:
                for url in entry['urls']:
                    pos = get_pattern_position(url, entry['tweet'])
                    insert_data('url', ('tweet_id', 'expanded_url', 'indices_start', 'indices_end', 'url'),
                                (entry['id'], url, *pos, url))

            if entry['mentions']:
                for mention in entry['mentions']:
                    if entry_exists('users', 'screen_name', 'screen_name', mention):
                        user = get_data('users', 'screen_name', mention)
                        user_id = user[0]
                        user_name = user[1]
                    else:
                        try:
                            # insert new user
                            user = twitter.get_user(screen_name=mention)
                            # save_user_to_db(conn.cursor(), user)
                            insert_data('users', (
                                'id', 'id_str', 'name', 'screen_name', 'location', 'url', 'description', 'is_verified',
                                'followers_count', 'friends_count', 'listed_count', 'favourites_count',
                                'statuses_count',
                                'created_at', 'is_geo_enabled', 'lang', 'is_history_complete'),
                                        (user.id, user.id_str, user.name, user.screen_name, user.location, user.url,
                                         user.description,
                                         user.verified, user.followers_count, user.friends_count, user.listed_count,
                                         user.favourites_count,
                                         user.statuses_count, user.created_at, user.geo_enabled, user.lang, 0))
                            user_id = user.id
                            user_name = user.name
                            conn.commit()
                        except TweepError:
                            user_id = -1
                            user_name = ''
                    # insert @
                    pos = get_pattern_position(mention, entry['tweet'])
                    insert_data('user_mention', (
                        'tweet_id', 'user_mention_id', 'user_mention_id_str', 'indices_start', 'indices_end', 'name',
                        'screen_name'), (entry['id'], user_id, str(user_id), *pos, user_name, mention))

            if entry['photos']:
                for pic in entry['photos']:
                    pos = get_pattern_position(pic, entry['tweet'], is_picture=True)
                    display_url = entry['tweet'][pos[0]:pos[1]]
                    insert_data('media', (
                        'tweet_id', 'display_url', 'media_id', 'indices_start', 'indices_end', 'media_url', 'type'),
                                (entry['id'], display_url, -1, *pos, pic, 'photo'))


def insert_data(table: str, columns, values):
    query = "INSERT OR IGNORE INTO " + table + "(" + ', '.join(columns) + ") VALUES (" + '?,' * (
            len(columns) - 1) + "?)"
    conn.execute(query, values)
    # conn.commit()


def update_users_to_latest_tweet():
    df = queries.load_latest_tweets()
    for idx, row in df.iterrows():
        try:
            scrape_user_profile(row['bounding_box_id'], row['screen_name'], last_tweet_id=row['last_tweet'])
        except (ServerDisconnectedError):
            print("passed users profile: " + entry[1])
            pass
        conn.commit()


if __name__ == '__main__':
    # important user paradox = ["maastrichtu", "uitmaastricht", "mecc_maastricht"]
    authenticate_tweepy()
    create_database_connection()
    update_users_to_latest_tweet()

    bounding_boxes = get_location_bounding_box()
    user_screen_names, user_locations, user_ids = get_users()
    # define pattern to remove all chars but alphanumeric ones from location
    p = re.compile(r'[\W_]+')
    valid_screen_names = []
    limburg_options = [31, 32, 33, 34, 35, 36]
    for screen_name, location, user_id in zip(user_screen_names, user_locations, user_ids):
        # if user_id % 3 == segment_id:
        location = location.lower()
        location_exclude = [r"bergen.*norway"]
        for excl in location_exclude:
            if re.search(r"\b" + excl + r"\b", location):
                break
        else:
            for bounding_box in bounding_boxes:
                if re.search(r"\b" + bounding_box[1].lower() + r"\b", location):
                    valid_screen_names.append((bounding_box[0], screen_name, location))
            if re.search(r"\blimburg\b", location):
                valid_screen_names.append((random.choice(limburg_options), screen_name, location))

    # TODO 1078759526441148416 reply parse from webpage
    # https://medium.com/@alexisperrier/topic-modeling-of-twitter-timelines-in-python-bb91fa90d98d
    for entry in valid_screen_names:
        try:
            scrape_user_profile(entry[0], entry[1])
        except (ServerDisconnectedError):
            print("passed users profile: " + entry[1])
            pass
        conn.execute('UPDATE users SET is_history_complete = 1 WHERE screen_name=?', [entry[1]])
        conn.commit()
        # shutil.rmtree('tweets/' + username)

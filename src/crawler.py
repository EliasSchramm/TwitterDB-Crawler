from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database
from requests.api import patch

import _secrets

import requests
import json
import schedule
import datetime

from tweet import Tweet
from threading import Thread

TOTAL_TAGS = 0
TOTAL_HASHTAGS = 0
TOTAL_TWEETS = 0
TOTAL_RETWEETS = 0

SAMPLE_TWEET_INTERVALL = 25

DB = None


def _get_current_timestamp() -> int:
    now = datetime.datetime.now()
    return int(
        datetime.datetime(
            year=now.year, month=now.month, day=now.day, hour=now.hour
        ).timestamp()
    )


def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream"


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint():
    url = create_url()
    headers = create_headers(_secrets.TWITTER_API_KEY)

    schedule.every(2).seconds.do(_spawn_save_totals)
    schedule.every().hour.at(":55").do(_spawn_calculate_tops)

    response = requests.request("GET", url, headers=headers, stream=True)

    tweet_sample_count = 0
    for response_line in response.iter_lines():
        if response_line:
            if b"data" in response_line:
                json_response = json.loads(response_line)
                t = Thread(
                    target=_handle_tweet,
                    args=(json_response["data"]["text"], _get_current_timestamp()),
                )
                t.start()

                if tweet_sample_count % SAMPLE_TWEET_INTERVALL == 0:
                    DB["sampled_tweets"].insert_one(
                        {
                            "timestamp": int(
                                datetime.datetime.now().timestamp() * 1000
                            ),
                            "tweet": json_response,
                        }
                    )
                tweet_sample_count += 1

                schedule.run_pending()
        if response.status_code != 200:
            print(response.status_code)
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )


def _handle_tweet(text: str, timestamp: str):
    global TOTAL_TWEETS, TOTAL_RETWEETS, TOTAL_HASHTAGS, TOTAL_TAGS, DB

    t = Tweet(text)

    TOTAL_TWEETS += 1
    if text.startswith("RT"):
        TOTAL_RETWEETS += 1

    TOTAL_TAGS += len(t.tags)
    TOTAL_HASHTAGS += len(t.hashtags)

    tag_collection = None
    if t.tags:
        tag_collection = DB["tags"]

    hashtag_collection = None
    if t.hashtags:
        hashtag_collection = DB["hashtags"]

    for tag in t.tags:
        entry = {"name": tag, "timeline": [{"count": 1, "timestamp": timestamp}]}
        existing_entry = tag_collection.find_one({"name": tag})
        if existing_entry:
            if existing_entry["timeline"][0]["timestamp"] == timestamp:
                existing_entry["timeline"][0]["count"] += 1
            else:
                existing_entry["timeline"] = [
                    {"count": 1, "timestamp": timestamp}
                ] + existing_entry["timeline"]
            entry = existing_entry

        entry["datapoints"] = len(entry["timeline"])

        tag_collection.update_one({"name": tag}, {"$set": entry}, upsert=True)

    for hashtag in t.hashtags:
        entry = {"name": hashtag, "timeline": [{"count": 1, "timestamp": timestamp}]}
        existing_entry = hashtag_collection.find_one({"name": hashtag})
        if existing_entry:
            if existing_entry["timeline"][0]["timestamp"] == timestamp:
                existing_entry["timeline"][0]["count"] += 1
            else:
                existing_entry["timeline"] = [
                    {"count": 1, "timestamp": timestamp}
                ] + existing_entry["timeline"]
            entry = existing_entry

        entry["datapoints"] = len(entry["timeline"])

        hashtag_collection.update_one({"name": hashtag}, {"$set": entry}, upsert=True)


def _load_totals():
    global TOTAL_TWEETS, TOTAL_RETWEETS, TOTAL_HASHTAGS, TOTAL_TAGS, DB

    col = DB["totals"]
    latest: Cursor = col.find({}).sort("_id", -1).limit(1)

    if latest.count():
        TOTAL_HASHTAGS = latest[0]["count_hashtags"]
        TOTAL_TAGS = latest[0]["count_tags"]
        TOTAL_TWEETS = latest[0]["count_tweets"]
        TOTAL_RETWEETS = latest[0]["count_retweets"]


def _spawn_save_totals():
    t = Thread(
        target=_save_totals,
    )
    t.start()


def _save_totals():
    col = DB["tags"]
    uniqueTags = col.count()

    col = DB["hashtags"]
    uniqueHashTags = col.count()

    col = DB["totals"]
    col.update_one(
        {"timestamp": _get_current_timestamp()},
        {
            "$set": {
                "timestamp": _get_current_timestamp(),
                "count_retweets": TOTAL_RETWEETS,
                "count_tweets": TOTAL_TWEETS,
                "count_tags": TOTAL_TAGS,
                "count_hashtags": TOTAL_HASHTAGS,
                "unique_tags": uniqueTags,
                "unique_hashtags": uniqueHashTags,
            }
        },
        upsert=True,
    )


def _spawn_calculate_tops():
    t = Thread(
        target=_calculate_tops,
    )
    t.start()


def _calculate_tops():
    t = _get_current_timestamp()
    col = DB["tags"]
    top_tags = (
        col.find({"timeline.0.timestamp": t}).sort("timeline.0.count", -1).limit(100)
    )
    _top_tags = []
    for x in top_tags:
        _top_tags.append({"name": x["name"], "count": x["timeline"][0]["count"]})

    col = DB["hashtags"]
    top_hashtags = (
        col.find({"timeline.0.timestamp": t}).sort("timeline.0.count", -1).limit(100)
    )
    _top_hashtags = []
    for x in top_hashtags:
        _top_hashtags.append({"name": x["name"], "count": x["timeline"][0]["count"]})

    col = DB["top"]
    col.insert_one({"timestamp": t, "tags": _top_tags, "hashtags": _top_hashtags})


if __name__ == "__main__":
    mongoClient = MongoClient(_secrets.MONGO_DB_URI)
    DB = mongoClient["TwitterDB"]
    _load_totals()

    connect_to_endpoint()

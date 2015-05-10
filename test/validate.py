from pymongo import MongoClient
import pymongo
import argparse
import datetime
import calendar
from bson import ObjectId

DESCRIPTION = 'validates the reports'

args = None
db = None
metrics_db = None

def get_options():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('mongodb', help='db to connect to i.e. mongodb://127.0.0.1:27017')
    parser.add_argument('merchant_id', help='merchant id to validate i.e. 54fe13816f5e2268db95caf1')
    parser.add_argument('month', help='merchant id to validate i.e. 2015Mar')
    return parser.parse_args()

def parse_args():
    global args
    args = get_options()

def connect_db():
    print 'Connecting to', args.mongodb
    global db
    global metrics_db
    client = MongoClient(args.mongodb)
    db = client.localmeasure
    metrics_db = client.localmeasure_metrics

def setup():
    parse_args()
    connect_db()

def validate_content(merchant_id, month):
    erroneous_reports = []
    content_reports = metrics_db.content.find({'merchant_id': merchant_id})

    for cr in content_reports:
        places = db.places.find({'name': cr['place_name']})
        venues = []
        for place in places:
            for venue in place['venue_ids']:
                if venue not in venues:
                    venues.append(venue)

        if not venues:
            print 'no venues to query for this place name'
            continue

        start = datetime.datetime.strptime(month + '01', "%Y%b%d")
        end = datetime.datetime.strptime(month + '31', "%Y%b%d")

        post_count = db.posts.find({'secondary_venue_ids': {'$in': venues}, 
                                    'post_time': {'$gt': start, '$lt': end}}).count()

        print 'posts found {} : {} at {}'.format(post_count, cr['total'], cr['place_name'])

def find_all_mentions(merchant_id):
    mentions = []
    places = db.places.find({'merchant_id': ObjectId(merchant_id)})
    for place in places:
        for venue_id in place['venue_ids']:
            venue = db.venues.find_one({'_id': venue_id})
            if venue and 'type' in venue:
                if venue['type'] == 'mention':
                    mentions.append(venue['term'])

    print '{}'.format(mentions)

if __name__ == '__main__':
    setup()
    find_all_mentions(args.merchant_id)



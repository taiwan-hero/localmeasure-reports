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

months = ['2014Jun', '2014Jul', '2014Aug', '2014Sep', '2014Oct', '2014Nov', '2014Dec', '2015Jan', '2015Feb', '2015Mar', '2015Apr', '2015May']

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

    places = db.places.find({'merchant_id': ObjectId(merchant_id)})
    venues = []
    for place in places:
        for venue_id in place['venue_ids']:
            venues.append(venue_id)

    print "venue_ids = {}".format(venues)
    start = datetime.datetime.strptime(month + '01', "%Y%b%d")
    end = datetime.datetime.strptime(month + '31', "%Y%b%d")

    post_count = db.posts.find({'secondary_venue_ids': {'$in': venues}, 
                                'post_time': {'$gt': start, '$lt': end}}).count()

    print 'posts found {} '.format(post_count)

def find_all_venue_mentions(merchant_id):
    mentions = []
    places = db.places.find({'merchant_id': ObjectId(merchant_id)})
    for place in places:
        for venue_id in place['venue_ids']:
            venue = db.venues.find_one({'_id': venue_id})
            if venue and 'type' in venue:
                if venue['type'] == 'mention':
                    mentions.append(venue['term'])

    return mentions

def find_all_handle_mentions(merchant_id):
    mentions = []
    merchant = db.merchants.find_one({'_id': ObjectId(merchant_id)})
    if merchant and 'linked_accounts' in merchant:
        if 'instagram' in merchant['linked_accounts']:
            for ig in merchant['linked_accounts']['instagram']:
                mentions.append(ig['name'])

        if 'twitter' in merchant['linked_accounts']:
            for tw in merchant['linked_accounts']['twitter']:
                mentions.append(tw['name'])

    return mentions

def get_mentions(merchant_id, month, mentions):
    keyword_mentions = metrics_db.keywords.find({'merchant_id': merchant_id, 'post_month': month, 'word': {'$in': mentions}})
    for km in keyword_mentions:
        print '{} : {} : {}'.format(km['place_name'], km['word'], km['total'])

def validate_interactions(merchant_id, month):
    print month
    places = db.places.find({'merchant_id': ObjectId(merchant_id)})
    for place in places:
        venues = place['venue_ids']
        if not venues:
            continue
        start = datetime.datetime.strptime(month + '01', "%Y%b%d")
        next_month = int(start.strftime('%m')) + 1
        year = int(start.strftime('%Y'))
        if next_month == 13:
            next_month = 1
            year = year + 1

        end = datetime.datetime.strptime(str(year) + str(next_month).zfill(2) + '01', "%Y%m%d")

        post_count = db.posts.find({'secondary_venue_ids': {'$in': venues}, 
                                    'post_time': {'$gt': start, '$lt': end},
                                    'tag_ids': {'$in': [ObjectId("5433518d6f5e223cbd4dd921"), ObjectId("5433c142c62697253474f18c")]}
                                    }).count()

        print '  {} : {}'.format(place['name'], post_count)

def list_users_interactions(user_name, month):
    start = datetime.datetime.strptime(month + '01', "%Y%b%d")
    next_month = int(start.strftime('%m')) + 1
    year = int(start.strftime('%Y'))
    if next_month == 13:
        next_month = 1
        year = year + 1

    end = datetime.datetime.strptime(str(year) + str(next_month).zfill(2) + '01', "%Y%m%d")

    this_months_likes = db.audits.find({'actor.label': user_name, 'type': 'like', 'created_at': {'$gt': start, '$lt': end}})

    for like in this_months_likes:
        orig_post = db.posts.find_one({'_id': like['subject']['origin_id']})
        if not orig_post:
            continue
        print 'liked: {} at:'.format(orig_post['_id'])
        venues = orig_post['secondary_venue_ids']
        post_places = db.places.find({'venue_ids': {'$in': venues}})
        for post_place in post_places:
            print '  place: {} - {}'.format(post_place['name'], post_place['merchant_id'])

def validate_segment_totals():
    for segment in metrics_db.segments.find({}):
        totals = segment['counts']['total']
        sums = {"FB": 0, "IG": 0, "4S": 0, "TW": 0}

        for i in range(24):
            for source in sums:
                sums[source] += segment['counts']["%02d"%i][source]

        for source in sums:
            print '{} {}'.format(sums[source], totals[source])
            if sums[source] != totals[source]:
                print 'problem'

if __name__ == '__main__':
    setup()
    validate_segment_totals()





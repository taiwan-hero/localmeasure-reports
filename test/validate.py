from pymongo import MongoClient
import pymongo
import argparse
import datetime
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
    content_reports = metrics_db.content.find({})

    for cr in content_reports:
        calc_total = 0
        for source in cr['counts']:
            for content_type in cr['counts'][source]:
                calc_total += cr['counts'][source][content_type]

        if cr['total'] != calc_total:
            print 'ERROR: {} / {} / {} does not have correct total'.format(cr['place_name'].encode('utf-8'), cr['total'], calc_total)

    venues = []
    places = db.places.find({'merchant_id': ObjectId(merchant_id)})
    for place in places:
        for venue in place['venue_ids']:
            if venue not in venues:
                venues.append(venue)

    print venues

if __name__ == '__main__':
    setup()
    validate_content(args.merchant_id, args.month)

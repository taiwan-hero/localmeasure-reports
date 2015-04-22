from pymongo import MongoClient
import pymongo
import argparse
import datetime
from bson import ObjectId

DESCRIPTION = 'connecting to mongo DBs and doing stuff'

args = None
db = None

def get_options():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('mongodb', help='db to connect to i.e. mongodb://127.0.0.1:27017')
    return parser.parse_args()

def parse_args():
    global args
    args = get_options()

def connect_db():
    print 'Connecting to', args.mongodb
    global db
    client = MongoClient(args.mongodb)
    db = client.localmeasure

def get_db():
    return db

def setup():
    parse_args()
    connect_db()

def create_merchant(name, expired=False):
    if expired:
        expires_at = datetime.datetime.now()
        expires_at = expires_at - datetime.timedelta(days=10)
    else:
        expires_at = datetime.datetime.now()
        expires_at = expires_at + datetime.timedelta(days=10)

    return db.merchants.insert_one({'name': name, 'subscription': {'expires_at': expires_at}})

def create_place_for_merchant(merchant_id, place_name, venue_ids):
    return db.places.insert_one({'name': place_name, 'merchant_id': merchant_id, 'venue_ids': venue_ids})

def create_post(post_id, secondary_venue_ids, post_time, text):
    posted_at = datetime.datetime.strptime(post_time, "%d/%m/%y %H:%M")
    return db.posts.insert_one({'_id': post_id, 'secondary_venue_ids': secondary_venue_ids, 'post_time': posted_at, 'text': text})

def create_audit(category, audit_type, merchant_id, audit_time, user_id, user_name):
    created_at = datetime.datetime.strptime(audit_time, "%d/%m/%y %H:%M")
    return db.audits.insert_one({'category': category, 'type': audit_type, 'merchant_id': merchant_id, 'created_at': created_at, })
def clear_all_merchants():
    db.merchants.delete_many({})

def clear_all_places():
    db.places.delete_many({})

def clear_all_posts():
    db.posts.delete_many({})

def clear_all_audits():
    db.audits.delete_many({})

if __name__ == '__main__':
    setup()
    clear_all_merchants()
    clear_all_places()
    clear_all_posts()

    result = create_merchant("timcorpo")
    create_place_for_merchant(result.inserted_id, 'Tims House', ['FB-1', 'TW-1', 'IG-1','FB-11', 'FB-111'])
    create_place_for_merchant(result.inserted_id, 'Local Measure', ['FB-2', 'TW-2', 'IG-2'])
    create_place_for_merchant(result.inserted_id, 'Fitness First', ['FB-3', 'TW-3', 'IG-3'])

    create_post('FB-aa', ['FB-1', 'FB-11', 'FB-111'], '21/01/15 16:30', 'The cat sat on the mat')
    create_post('IG-bb', ['IG-1', 'IG-11', 'IG-111'], '21/01/15 17:30', 'the quick brown Cat jumped over the lazy cat')
    create_post('TW-cc', ['TW-1', 'TW-11', 'TW-111'], '21/01/15 18:30', 'Lydias cat is overweight')

    create_post('FB-dd', ['FB-2', 'FB-22', 'FB-222'], '22/01/15 16:30', 'Dave likes Apples')
    create_post('IG-ee', ['IG-2', 'IG-22', 'IG-222'], '22/01/15 17:30', 'can you please watch my Apples?')
    create_post('TW-ff', ['TW-2', 'TW-22', 'TW-222'], '22/01/15 18:30', 'Barrys Apple is small')

    create_post('FB-gg', ['FB-1', 'FB-11', 'FB-111'], '23/01/15 16:30', 'The cat sat on the mat')
    create_post('IG-hh', ['IG-2', 'IG-22', 'IG-222'], '23/01/15 17:30', 'the quick brown fox jumped over the lazy dog')
    create_post('TW-ii', ['TW-3', 'TW-33', 'TW-333'], '23/01/15 18:30', 'Freds dog is cute')

    create_post('FB-jj', ['FB-1', 'FB-11', 'FB-111'], '04/04/15 16:30', 'The cat sat on the mat')
    create_post('IG-kk', ['IG-2', 'IG-22', 'IG-222'], '04/04/15 17:30', 'the quick brown fox jumped over the lazy dog')
    create_post('TW-ll', ['TW-3', 'TW-33', 'TW-333'], '04/04/15 18:30', 'Sallys cat is dumb')

    create_post('FB-mm', ['FB-1', 'FB-11', 'FB-111'], '05/04/15 16:30', 'The cat sat on the mat')
    create_post('IG-nn', ['IG-2', 'IG-22', 'IG-222'], '05/04/15 17:30', 'the quick brown fox jumped over the lazy dog')
    create_post('TW-oo', ['TW-3', 'TW-33', 'TW-333'], '05/04/15 18:30', 'Pauls frog is overweight')
















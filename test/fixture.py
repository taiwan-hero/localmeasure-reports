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
    return db.places.insert_one({'name': place_name, 
                                'merchant_id': merchant_id, 
                                'venue_ids': venue_ids})

def create_post(post_id, secondary_venue_ids, post_time, text, kind):
    posted_at = datetime.datetime.strptime(post_time, "%d/%m/%y %H:%M")
    return db.posts.insert_one({'_id': post_id, 
                                'secondary_venue_ids': secondary_venue_ids, 
                                'post_time': posted_at, 
                                'text': text, 
                                'kind': kind})

def create_review(post_id, secondary_venue_ids, post_time, value):
    posted_at = datetime.datetime.strptime(post_time, "%d/%m/%y %H:%M")
    return db.posts.insert_one({'_id': post_id, 
                                'secondary_venue_ids': secondary_venue_ids, 
                                'post_time': posted_at, 
                                'rating': {'value': value, 'scale': 5}, 
                                'kind': 'review'})

def create_audit(category, audit_type, merchant_id, audit_time, user_name, post_id):
    created_at = datetime.datetime.strptime(audit_time, "%d/%m/%y %H:%M")
    return db.audits.insert_one({'category': category, 
                                'type': audit_type, 
                                'merchant_id': merchant_id, 
                                'created_at': created_at, 
                                'actor': {'type': 'user', 
                                            'object_id': 'blah', 
                                            'label': user_name},
                                'subject': {'type': 'post', 
                                            'origin_id': post_id, 
                                            'label': 'blah'}
                                })

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
    clear_all_audits()

    result = create_merchant("timcorpo")
    create_place_for_merchant(result.inserted_id, 'Tims House', ['FB-1', 'TW-1', 'IG-1','FB-11', 'FB-111'])
    create_place_for_merchant(result.inserted_id, 'Local Measure', ['FB-2', 'TW-2', 'IG-2'])
    create_place_for_merchant(result.inserted_id, 'Fitness First', ['FB-3', 'TW-3', 'IG-3'])

    create_post('FB-aa', ['FB-1', 'FB-11', 'FB-111'], '21/01/15 16:30', 'The cat sat on the mat', 'photo')
    create_post('IG-bb', ['IG-1', 'IG-11', 'IG-111'], '21/01/15 17:30', 'the quick brown Cat jumped over the lazy cat', 'photo')
    create_post('TW-cc', ['TW-1', 'TW-11', 'TW-111'], '21/01/15 18:30', 'Lydias cat is overweight', 'text')

    create_post('FB-dd', ['FB-2', 'FB-22', 'FB-222'], '22/01/15 16:30', 'Dave likes Apples', 'video')
    create_post('IG-ee', ['IG-2', 'IG-22', 'IG-222'], '22/01/15 17:30', 'can you please watch my Apples?', 'photo')
    create_post('TW-ff', ['TW-2', 'TW-22', 'TW-222'], '22/01/15 18:30', 'Barrys Apple is small', 'text')

    create_post('FB-gg', ['FB-1', 'FB-11', 'FB-111'], '23/01/15 16:30', 'The cat sat on the mat', 'photo')
    create_post('IG-hh', ['IG-2', 'IG-22', 'IG-222'], '23/01/15 17:30', 'the quick brown fox jumped over the lazy dog', 'photo')
    create_post('TW-ii', ['TW-3', 'TW-33', 'TW-333'], '23/01/15 18:30', 'Freds dog is cute', 'text')

    create_post('FB-jj', ['FB-1', 'FB-11', 'FB-111'], '04/04/15 16:30', 'The cat sat on the mat', 'photo')
    create_post('IG-kk', ['IG-2', 'IG-22', 'IG-222'], '04/04/15 17:30', 'the quick brown fox jumped over the lazy dog', 'photo')
    create_post('TW-ll', ['TW-3', 'TW-33', 'TW-333'], '04/04/15 18:30', 'Sallys cat is dumb', 'text')

    create_post('FB-mm', ['FB-1', 'FB-11', 'FB-111'], '05/04/15 16:30', 'The cat sat on the mat', 'photo')
    create_post('IG-nn', ['IG-2', 'IG-22', 'IG-222'], '05/04/15 17:30', 'the quick brown fox jumped over the lazy dog', 'photo')
    create_post('TW-oo', ['TW-3', 'TW-33', 'TW-333'], '05/04/15 18:30', 'Pauls frog is overweight', 'text')

    create_review('FB-pp', ['FB-1', 'FB-11', 'FB-111'], '05/04/15 16:30', '2')
    create_review('FB-qq', ['IG-2', 'IG-22', 'IG-222'], '05/04/15 17:30', '3')
    create_review('FB-rr', ['TW-3', 'TW-33', 'TW-333'], '05/04/15 18:30', '4')
    create_review('FB-ss', ['TW-3', 'TW-33', 'TW-333'], '05/04/15 18:30', '4')

    create_audit('interaction', 'like', result.inserted_id, '21/01/15 16:40', 'Tim Tang', 'FB-aa')
    create_audit('interaction', 'like', result.inserted_id, '21/01/15 16:45', 'Tim Tang', 'FB-aa')
    create_audit('interaction', 'like', result.inserted_id, '21/01/15 16:50', 'Tim Tang', 'FB-aa')
    create_audit('interaction', 'like', result.inserted_id, '21/01/15 17:40', 'Freddy Prinze Jr', 'IG-bb')
    create_audit('interaction', 'like', result.inserted_id, '21/01/15 18:40', 'Tim Tang', 'TW-cc')
    create_audit('interaction', 'like', result.inserted_id, '22/01/15 16:40', 'Freddy Prinze Jr', 'FB-dd')
    create_audit('interaction', 'like', result.inserted_id, '22/01/15 17:40', 'Tim Tang', 'IG-hh')
    create_audit('interaction', 'like', result.inserted_id, '22/01/15 18:40', 'Freddy Prinze Jr', 'TW-ii')
'''
db.audits.find({"actor.label":"Daniela Aravena", "type":{$in: ["like","reply"]},"created_at":{$gt:ISODate("2015-03-01T00:00:00.000Z"), $lt:ISODate("2015-04-01T00:00:00.000Z")}}).count()
'''

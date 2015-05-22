import os
import sys
from subprocess import call
from datetime import date
import pymongo
import argparse

print 'starting'

args = None
DB = None
DB_PORT = None
reports_home = '~/localmeasure-reports'

scripts = {'content':       reports_home+'/pigscripts/content_types.pig',
            'interactions': reports_home+'/pigscripts/interactions_users.pig',
            'posters':      reports_home+'/pigscripts/posters.pig',
            'reviews':      reports_home+'/pigscripts/content_reviews.pig',
            'keywords':     reports_home+'/pigscripts/content_keywords.pig'}


def _setup():
    global args
    global DB
    global DB_PORT
    
    parser = argparse.ArgumentParser(description='run a bunch of pig scripts')
    parser.add_argument('mongodb', help='db to connect to i.e. mongodb://127.0.0.1:27017')
    args = parser.parse_args()
    client = pymongo.MongoClient(args.mongodb)
    db = client.localmeasure_metrics
    DB_PORT = args.db_port

def _run_script(script, month):

    cmd = ['pig', '-x', 'local', 
                '-param', 'DB=localhost',
                '-param', 'MONTH=2015May', 
                '-f', 'pigscripts/content_reviews.pig']

    call(cmd, stderr=open('~/reports_error','w'), stdout=open('~/reports_output','w'))

if __name__ == '__main__':
    _setup()
    #get the date string for today
    today = date.today()
    this_month = today.strftime('%Y%b')
    print this_month
    _run_script('blah', 'foo')
    #write a document


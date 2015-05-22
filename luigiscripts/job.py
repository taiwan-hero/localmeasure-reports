import os
import sys
from subprocess import call
from datetime import date
import pymongo
import argparse

args = None
db = None
db_metrics = None
reports_home = '~/localmeasure-reports'

scripts = {'content':       reports_home+'/pigscripts/content_types.pig',
            'interactions': reports_home+'/pigscripts/interactions_users.pig',
            'posters':      reports_home+'/pigscripts/posters.pig',
            'reviews':      reports_home+'/pigscripts/content_reviews.pig',
            'keywords':     reports_home+'/pigscripts/content_keywords.pig'}


def _setup():
    global args
    global db
    
    parser = argparse.ArgumentParser(description='run a bunch of pig scripts')
    parser.add_argument('mongodb', help='db to connect to i.e. mongodb://127.0.0.1:27017')
    args = parser.parse_args()
    client = pymongo.MongoClient(args.mongodb)
    db = client.localmeasure
    db_metrics = client.localmeasure_metrics

def _run_script(script, month):

    cmd = ['pig', '-x', 'local', 
                '-param']

    cmd.append('DB=' + args.mongodb)
    cmd.append('-param')
    cmd.append('MONTH=' + month)
    cmd.append('-f')
    cmd.append('script')

    call(cmd)

if __name__ == '__main__':
    _setup()
    #get the date string for today
    today = date.today()
    this_month = today.strftime('%Y%b')
    print this_month
    _run_script(scripts['content'], this_month)
    #write a document


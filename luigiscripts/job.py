import os
import sys
from subprocess import call
from datetime import datetime, date
import pymongo
import argparse

args = None
db = None
db_metrics = None
reports_home = '/home/ubuntu/localmeasure-reports'
month = None
report = None

scripts = {'content':       reports_home+'/pigscripts/content_types.pig',
            'interactions': reports_home+'/pigscripts/interactions_users.pig',
            'posters':      reports_home+'/pigscripts/posters.pig',
            'reviews':      reports_home+'/pigscripts/content_reviews.pig',
            'keywords':     reports_home+'/pigscripts/content_keywords.pig',
            'segments':     reports_home+'/pigscripts/post_segments.pig'}


def _setup():
    global args
    global db
    global db_metrics
    global month
    global report

    parser = argparse.ArgumentParser(description='run a bunch of pig scripts')
    parser.add_argument('mongodb', help='db to connect to i.e. mongodb://127.0.0.1:27017')
    parser.add_argument('--month', help='month to run')
    parser.add_argument('--report', help='which report to run only')

    args = parser.parse_args()
    print 'connecting to: {}'.format(args.mongodb)
    client = pymongo.MongoClient(args.mongodb)
    if not client:
        print 'failed to connect to db'
    db = client.localmeasure
    db_metrics = client.localmeasure_metrics

    if args.month:
        month = args.month
    else:
        #get the date string for today
        now = datetime.now()
        today = date.today()
        month = today.strftime('%Y%b')

    print 'month = {}'.format(month)

    if args.report:
        report = args.report

    print 'report = {}'.format(report)

def _run_script(script, month):

    cmd = ['pig', '-x', 'local', 
                '-param']

    cmd.append('DB=' + args.mongodb)
    cmd.append('-param')
    cmd.append('MONTH=' + month)
    cmd.append('-f')
    cmd.append(script)

    print 'HADOOP: executing: {}'.format(cmd)
    call(cmd)

if __name__ == '__main__':
    _setup()

    if not report or report == 'content':
        #content types
        db_metrics.content.remove({'post_month': month})
        _run_script(scripts['content'], month)

    if not report or report == 'interactions':
        #interactions users
        db_metrics.interactions.remove({'month': month})
        _run_script(scripts['interactions'], month)

    if not report or report == 'posters':
        #posters
        db_metrics.posters.remove({'post_month': month})
        _run_script(scripts['posters'], month)

    if not report or report == 'reviews':
        #reviews
        db_metrics.reviews.remove({'month': month})
        _run_script(scripts['reviews'], month)

    if not report or report == 'keywords':
        #keywords
        db_metrics.terms.remove({'post_month': month})
        _run_script(scripts['keywords'], month)

    if not report or report == 'segments':
        #segments
        db_metrics.terms.remove({'month': month})
        _run_script(scripts['segments'], month)


    



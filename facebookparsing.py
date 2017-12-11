# -*- coding: utf-8 -*-
"""
Created on Fri Dec  8 06:29:56 2017

@author: suamatya
"""
""" Graph API URL
https://developers.facebook.com/tools/explorer/145634995501895?method=GET&path=ncell%2Ffeed%2F%3Ffields%3Dmessage%2Clink%2Ccreated_time%2Ctype%2Cname%2Cid%2Cfrom%2Clikes.limit(1)%2Ccomments.limit(1)%2Cshares%26limit%3D10&version=v2.10
"""

from __future__ import with_statement
import urllib
import json
import datetime
import csv
import time
import urllib.request
import sqlite3
from contextlib import closing
import re



sql_transaction = []
connection = sqlite3.connect('E:\\python\\notebook\\course3_downloads\\feed.db')

def createTabes():
    with closing(connection.cursor()) as cursor:
        cursor.execute("""create table if not exists main_post(status_id text, 
            status_message text,status_from text, link_name text, status_type text,
            status_link text,status_published DATETIME,
            num_likes int, num_comments int, num_shares int)""")
    
        cursor.execute("""create table if not exists comment(post_id text , 
            status_message text,status_from text, user_id text,
            comment_id text,comment_date DATETIME)""")

if __name__ == "__main__":
    createTabes()
   
    
app_id = "" #Your app id
app_secret = "" # DO NOT SHARE WITH ANYONE!

access_token = app_id + "|" + app_secret

page_id = 'ncell'

def request_until_succeed(url):
    req = urllib.request.Request(url)
    success = False
    while success is False:
        try: 
            response = urllib.request.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)            
            print("Error for URL %s: %s" % (url, datetime.datetime.now()))     
    return response.read()


def getFacebookPageFeedData(page_id, access_token, num_statuses):
    
    # construct the URL string
    base = "https://graph.facebook.com"
    node = "/" + page_id + "/feed" 
    parameters = "/?fields=message,link,created_time,type,name,id,from,likes.limit(1).summary(true),comments.limit(1).summary(true),shares&limit=%s&access_token=%s" % (num_statuses, access_token) # changed
    url = base + node + parameters
    
    # retrieve data
    data = json.loads(request_until_succeed(url))
    
    return data

def processFacebookPageFeedStatus(status):
    
    # The status is now a Python dictionary, so for top-level items,
    # we can simply call the key.
    
    # Additionally, some items may not always exist,
    # so must check for existence first
    
    status_id = status['id']
    status_message = '' if 'message' not in status.keys() else re.split('[.| ]//', status['message'])[0]
    #print(status_message)
    status_from = 'na' if 'from' not in status.keys() else status['from']['name']
    link_name = '' if 'name' not in status.keys() else status['name']
    status_type = status['type']
    status_link = '' if 'link' not in status.keys() else status['link']
    status_comments = '' if 'comments' not in status.keys() else status['comments']['data']
    
    
    # Time needs special care since a) it's in UTC and
    # b) it's not easy to use in statistical programs.
    
    status_published = datetime.datetime.strptime(status['created_time'],'%Y-%m-%dT%H:%M:%S+0000')
    status_published = status_published + datetime.timedelta(hours=-5) # EST
    status_published = status_published.strftime('%Y-%m-%d %H:%M:%S') # best time format for spreadsheet programs
    
    # Nested items require chaining dictionary keys.
    
    num_likes = 0 if 'likes' not in status.keys() else status['likes']['summary']['total_count']
    num_comments = 0 if 'comments' not in status.keys() else status['comments']['summary']['total_count']
    num_shares = 0 if 'shares' not in status.keys() else status['shares']['count']
    
    # return a tuple of all processed data
    return (status_id, status_message,status_from, link_name, status_type, status_link,
           status_published, num_likes, num_comments, num_shares),status_comments
            
def sql_insert_comment(comment):
    with closing(connection.cursor()) as cursor:        
        holders = ','.join('?' * len(comment))
        sql = 'INSERT INTO comment VALUES({0})'.format(holders)
        cursor.execute(sql, comment)

    

def sql_insert_post(main_post):
    with closing(connection.cursor()) as cursor: 
        holders = ','.join('?' * len(main_post))
        sql = 'INSERT INTO main_post VALUES({0})'.format(holders)
        cursor.execute(sql, main_post)

W
def organizeCommentToLine(main_post_id,comment):
    return (main_post_id,re.split('[.| ]//',comment['message'])[0],comment['from']['name'],comment['from']['id'],comment['id'],comment['created_time'])

def scrapeFacebookPageFeedStatus(page_id, access_token):
    with open('E:\\python\\notebook\\course3_downloads\\%s_facebook_statuses.csv' % page_id, 'w',encoding='utf-8') as file:
        w = csv.writer(file)
        w.writerow(["status_id", "status_message", "from","link_name", "status_type", "status_link","status_published", "num_likes", "num_comments", "num_shares"])
        
        has_next_page = True
        num_processed = 0   # keep a count on how many we've processed
        scrape_starttime = datetime.datetime.now()
        
        print("Scraping %s Facebook Page: %s\n" % (page_id, scrape_starttime))
        
        statuses = getFacebookPageFeedData(page_id, access_token, 100)
        
        while has_next_page:
            for status in statuses['data']:
                main_post,comments = processFacebookPageFeedStatus(status)
                #print("main post")
                #print(main_post)
                w.writerow(main_post)
                #print(type(main_post))
                sql_insert_post(main_post)
                #print("comments")
                for comment in comments:
                   org_comment = organizeCommentToLine(main_post[0],comment)
                   #print(type(org_comment))
                   sql_insert_comment(org_comment)
                
                # output progress occasionally to make sure code is not stalling
                num_processed += 1
                if num_processed % 1000 == 0:
                    print("%s Statuses Processed: %s" % (num_processed, datetime.datetime.now()))
                    
            # if there is no next page, we're done.
            if 'paging' in statuses.keys():
                statuses = json.loads(request_until_succeed(statuses['paging']['next']))
            else:
                has_next_page = False
                
        connection.commit()
        print("\nDone!\n%s Statuses Processed in %s" % (num_processed, datetime.datetime.now() - scrape_starttime))

scrapeFacebookPageFeedStatus(page_id,access_token)
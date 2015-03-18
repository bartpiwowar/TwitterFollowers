#!/usr/bin/env python

from twitter import *
import json
import psycopg2
from functools import partial
from sys import maxint
import sys
from TwitterRequest import make_twitter_request
from Twitter import oauth_login
import time

def get_user_profile(twitter_api, screen_names=None, user_ids=None):
   
    # Must have either screen_name or user_id (logical xor)
    assert (screen_names != None) != (user_ids != None), \
    "Must have screen_names or user_ids, but not both"
    
    items_to_info = {}

    items = screen_names or user_ids
    
    while len(items) > 0:
        try:
            # Process 100 items at a time per the API specifications for /users/lookup.
            # See https://dev.twitter.com/docs/api/1.1/get/users/lookup for details.
            
            items_str = ','.join([str(item) for item in items[:100]])
            items = items[100:]
    
            if screen_names:
                response = make_twitter_request(twitter_api.users.lookup,
                                                screen_name=items_str)
            else:  # user_ids
                response = make_twitter_request(twitter_api.users.lookup,
                                                user_id=items_str)
            if response:
                for user_info in response:
                    if screen_names:
                        items_to_info[user_info['screen_name']] = user_info
                    else:  # user_ids
                        items_to_info[user_info['id']] = user_info
        except:
            print "Unexpected error:", sys.exc_info()

    return items_to_info

def store_data (cur,conn, results,twitter_user):
    # build storage structure
    stored =[]
    for user in results:
        #print json.dumps(results, indent=4)
        stored+=[user]
        try:           
            cur.execute("UPDATE followers SET screen_name=%s, name=%s, description=%s,followers_count=%s,friends_count=%s,lang=%s,location=%s,geo_enabled=%s,profile_image_url_https=%s,expanded_url=%s,display_url=%s, statuses_count=%s ,created_at=%s  where twitter_user=%s AND follower_id=%s",
                        (results[user]['screen_name'],
                         results[user]['name'],
                         results[user]['description'],
                         results[user]['followers_count'],
                         results[user]['friends_count'],
                         results[user]['lang'],
                         results[user]['location'],
                         results[user]['geo_enabled'],
                         results[user]['profile_image_url_https'],
                         results[user]['url'] if results[user]['url'] else None,
                         results[user]['url'] if results[user]['url'] else None,
                         results[user]['statuses_count'],
                         results[user]['created_at'],
                         #time.mktime(time.strptime(results[user]['created_at'],"%a %b %d %H:%M:%S +0000 %Y")),
                         twitter_user,
                         user))
            conn.commit()
        except:
            print user
            print "Unexpected error:", sys.exc_info()
            print json.dumps(results[user], indent=4)
    print "Stored {0} followers for {1}".format(len(stored),twitter_user)
    return stored

def cleanup (cur,conn,dead_followers,twitter_user):
    
    for user in dead_followers:
        try:
            cur.execute("UPDATE followers SET dead_follower=%s where twitter_user=%s and follower_id=%s",('TRUE',twitter_user,user))
            conn.commit()
        except:
            print user
            print "Unexpected error:", sys.exc_info()
    print "Flagged {0} dead followers".format(len(dead_followers))

if __name__ == '__main__':
    
    # Twitter user 
    twitter_users = ['Chicopee_Resort']    
    #twitter_users = ['alisonvictoria3']
    
    #initialize twitter api
    twitter_api = oauth_login()    
    
    # open data base communications
    conn = psycopg2.connect("dbname=socialpeeks user=postgres")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    for twitter_user in twitter_users:
        # get followers that don't have info stored
        cur.execute("select follower_id from followers where twitter_user=%s and screen_name is null and dead_follower=FALSE Limit 100", (twitter_user,)) 
        rows = cur.fetchall()
        
        while len(rows) > 0:
            users = []
            for row in rows:
                users += row
            results = get_user_profile(twitter_api, user_ids=users) 
            #print json.dumps(results, indent=4)
            #store retrieved follower info 
            stored=store_data(cur,conn,results,twitter_user)
            
            #cleanup dead followers
            cleanup(cur,conn,set(users)-set(stored),twitter_user)
            
            # get followers that don't have info stored
            cur.execute("select follower_id from followers where twitter_user=%s and screen_name is null and dead_follower=FALSE Limit 100", (twitter_user,)) 
            rows = cur.fetchall()
    
    print "Done!!"
    conn.close()
    
# Name
# of Followers
# Following
# of Tweets
# How long on Twitter    

# ALTER TABLE followers ADD COLUMN screen_name text;
# ALTER TABLE followers ADD COLUMN name text;
# ALTER TABLE followers ADD COLUMN description text;
# ALTER TABLE followers ADD COLUMN followers_count integer;
# ALTER TABLE followers ADD COLUMN friends_count integer;
# ALTER TABLE followers ADD COLUMN lang text;
# ALTER TABLE followers ADD COLUMN location text;
# ALTER TABLE followers ADD COLUMN geo_enabled boolean;
# ALTER TABLE followers ADD COLUMN profile_image_url_https text;
# ALTER TABLE followers ADD COLUMN expanded_url text;
# ALTER TABLE followers ADD COLUMN display_url text;
# ALTER TABLE followers ADD COLUMN statuses_count integer; 
# ALTER TABLE followers ADD COLUMN created_at time; 




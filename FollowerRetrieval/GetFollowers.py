#!/usr/bin/env python
from twitter import *
import json
import psycopg2
from functools import partial
from sys import maxint
import sys
from TwitterRequest import make_twitter_request
import time
from Twitter import oauth_login

# CREATE TABLE followers (
#     id     serial PRIMARY KEY,
#     twitter_user    text NOT NULL,
#     follower_id bigint NOT NULL,
#     UNIQUE (twitter_user, follower_id)
# );

def get_followers_ids(twitter_api, screen_name=None, user_id=None,
                              followers_limit=maxint):
    
    # Must have either screen_name or user_id (logical xor)
    assert (screen_name != None) != (user_id != None), \
    "Must have screen_name or user_id, but not both"
    
    # See https://dev.twitter.com/docs/api/1.1/get/friends/ids and
    # https://dev.twitter.com/docs/api/1.1/get/followers/ids for details
    # on API parameters
    
    get_followers_ids = partial(make_twitter_request, twitter_api.followers.ids, 
                                count=5000)

    followers_ids = []
    
    for twitter_api_func, limit, ids, label in [ 
                    [get_followers_ids, followers_limit, followers_ids, "followers"]
                ]:
        
        if limit == 0: continue
        
        cursor = -1
                
        while cursor != 0:
        
            # Use make_twitter_request via the partially bound callable...
            if screen_name: 
                response = twitter_api_func(screen_name=screen_name, cursor=cursor)
            else: # user_id
                response = twitter_api_func(user_id=user_id, cursor=cursor)

            if response is not None:
                ids += response['ids']
                cursor = response['next_cursor']
        
            print >> sys.stderr, 'Fetched {0} total {1} ids for {2}'.format(len(ids), 
                                                    label, (user_id or screen_name))
        
            # XXX: You may want to store data during each iteration to provide an 
            # an additional layer of protection from exceptional circumstances
            
            if len(ids) >= limit or response is None:
                break

    # Do something useful with the IDs, like store them to disk...
    return followers_ids[:followers_limit]

def store_data (cursor,followers_ids,user):
    start = time.time()
    for follower in followers_ids:
        try:
            cursor.execute("INSERT INTO followers (twitter_user,follower_id) VALUES(%s,%s)", (user,follower))
            conn.commit()
        except psycopg2.IntegrityError as ex:
            print ex
        except psycopg2.InternalError as ex:
            print ex                 
    
    end = time.time()
    print >> sys.stderr, 'Stored {0} follower ids'.format(len(followers_ids))
    print "Time of db inserts: ", end - start


if __name__ == '__main__':
    # Sample usage
    twitter_users=['Make_It_Right',]
    twitter_user = 'JOHNGIDDING'
    
    twitter_api = oauth_login()    
    
    # open data base communications
    conn = psycopg2.connect("dbname=socialpeeks user=postgres")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    # fetch all followers
    followers_ids = get_followers_ids(twitter_api, screen_name=twitter_user)
    print 'Total followers fetched: ', len(followers_ids)
    store_data(cur,set(followers_ids),twitter_user)
    
    # Close communication with the database
    conn.close()
    
# Property Brothers
# @PropertyBrother
# 
# David Bromstad X
# 
# Mike Holmes X
# 
# John Gidding X
# 
# Chris Lambton X
# 
# Vern Yip X
# 
# Emily Henderson X
# 
# Sarah Richardson X
# 
# Genevieve Gorder X
# 
# Sabrina Soto #
# 
# Alison Victoria X
# 
# Drew Scott X
# @MrDrewScott
# 
# Jonathan Scott
# @MrSilverScott    
    
    
    
    
    

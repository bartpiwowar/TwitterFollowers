#!/usr/bin/env python
from twitter import *
import json
import pymongo  # pip install pymongo
from functools import partial
from sys import maxint
import sys
from TwitterRequest import make_twitter_request
import time
from Twitter import oauth_login

def followers_ids_to_mongodb(collection, twitter_api, screen_name=None, user_id=None,
                              friends_limit=maxint, followers_limit=maxint, database=None):
 
    # Must have either screen_name or user_id (logical xor)
    assert (screen_name != None) != (user_id != None), 'Must have screen_name or user_id, but not both'
 
    # See https://dev.twitter.com/docs/api/1.1/get/friends/ids  and
    # See https://dev.twitter.com/docs/api/1.1/get/followers/ids for details on API parameters

    get_followers_ids = partial(make_twitter_request, twitter_api.followers.ids, count=5000)
 
    # reset the collection before collecting follwer ids
    clean_mongo_collection(database, collection)
    
    total_saved_followers = 0
    
    for twitter_api_func, limit, ids, label in [
                                 [get_followers_ids, followers_limit, total_saved_followers, 'followers']]:
 
        if limit == 0: continue
 
        total_ids = 0
        cursor = -1
        while cursor != 0:
 
            # Use make_twitter_request via the partially bound callable...
            if screen_name:
                response = twitter_api_func(screen_name=screen_name, cursor=cursor)
            else:  # user_id
                response = twitter_api_func(user_id=user_id, cursor=cursor)
 
            if response is not None:
                ids = response['ids']
                total_ids += len(ids)
                total_saved_followers += save_to_mongo(ids, database, collection)
                cursor = response['next_cursor']
 
            print >> sys.stderr, 'Fetched {0} total {1} ids for {2}'.format(total_ids, label, (user_id or screen_name))
            sys.stderr.flush()
 
            # Consider storing the ids to disk during each iteration to provide an
            # an additional layer of protection from exceptional circumstances
 
            if len(ids) >= limit or response is None:
                break
                print >> sys.stderr, 'Last cursor', cursor
                print >> sys.stderr, 'Last response', response
    
    return total_saved_followers

def get_user_profile(twitter_api, screen_names=None, user_ids=None):
   
    # Must have either screen_name or user_id (logical xor)
    assert (screen_names != None) != (user_ids != None), "Must have screen_names or user_ids, but not both"
    
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

def save_to_mongo(data, mongo_db, mongo_db_coll, **mongo_conn_kw):
    
    # Connects to the MongoDB server running on 
    # localhost:27017 by default    
    client = pymongo.MongoClient(**mongo_conn_kw)
    
    # Get a reference to a particular database
    db = client[mongo_db]
    
    # Reference a particular collection in the database
    coll = db[mongo_db_coll] 
    # Perform a bulk insert and  return the IDs
    
    # return coll.insert_many([{'id': i} for i in data])
    count = 0
    for value in data:
        try:
            coll.insert({"_id":value})
            count += 1
        except pymongo.errors.DuplicateKeyError:
            print >> sys.stderr, 'duplicate twitter user id ', value
        except pymongo.errors.PyMongoError as e:
            print >> sys.stderr, 'Failed to save follower id ', value, e
    
    client.disconnect()    
        
    return count

def clean_mongo_collection(mongo_db, mongo_db_coll, **mongo_conn_kw):
    
    # Connects to the MongoDB server running on 
    # localhost:27017 by default
    
    # Get a reference to a particular database
    client = pymongo.MongoClient(**mongo_conn_kw)
    collection = client[mongo_db][mongo_db_coll]
    
    # remove all documents
    if collection.count() > 0:
        collection.remove({})
     
    client.disconnect()    

def get_followers_from_mongo(mongo_db, mongo_db_coll, **mongo_conn_kw):
    # Connects to the MongoDB server running on 
    # localhost:27017 by default
    
    # Get a reference to a particular database
    client = pymongo.MongoClient(**mongo_conn_kw)
    collection = client[mongo_db][mongo_db_coll]
    
    # get all documents from collection
    cursor = collection.find(timeout=False)
    
    client.disconnect()    
    
    return cursor

def update_follower_info(data,mongo_db, mongo_db_coll, **mongo_conn_kw):
    # Connects to the MongoDB server running on 
    # localhost:27017 by default
    
    # Get a reference to a particular database
    client = pymongo.MongoClient(**mongo_conn_kw)
    collection = client[mongo_db][mongo_db_coll]
    
    # update documents with profile info
    # return coll.insert_many([{'id': i} for i in data])
    for key in data:
        try:
            collection.update({"_id":key},{"$set":data[key]} , upsert=True)
        except pymongo.errors.DuplicateKeyError:
            print >> sys.stderr, 'duplicate twitter user id ', key
        except pymongo.errors.PyMongoError as e:
            print >> sys.stderr, 'Failed to save follower id ', key, e
    
    client.disconnect()    
    

def get_follower_profiles(screen_name,app_user,twitter_oauth):
    
    collection = '{0}_{1}'.format(app_user, screen_name)
    database = 'twitter_followers'
     
    # step 1: collect all follower ids
    print >> sys.stderr, 'Getting follower ids for {0}'.format(collection)
    result = followers_ids_to_mongodb(collection, twitter_oauth, screen_name=screen_name,
                                friends_limit=0, database='twitter_followers')    
    print >> sys.stderr, 'Saved {0} follower ids'.format(result)
     
    # step 2: get follower info
    cursor = get_followers_from_mongo(database, collection)
    ids = []
    i = 0
    for item in cursor:      
        ids.append(item['_id'])
        i += 1
        if len(ids) == 100 or cursor.count() == i:
            #print '{0} {1}'.format(len(ids), i)
            results = get_user_profile(twitter_oauth, user_ids=ids) 
            update_follower_info(results,database, collection)
            #print json.dumps(result, indent=4)
            ids = []    
    print >> sys.stderr, 'Updated {0} follower profiles for {1}'.format(i,collection)
    cursor.close()
    

if __name__ == '__main__':
    
    # Sample usage
    screen_name = 'green_lemonnn'
    app_user = 'bart'
    
     # twitter user tokens
    twitter_oauth = oauth_login() 
  
    # get twitter user's followers info 
    get_follower_profiles(screen_name.lower(),app_user,twitter_oauth)
    

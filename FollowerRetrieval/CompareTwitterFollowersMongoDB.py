#!/usr/bin/env python
from twitter import *
import json
import pymongo  # pip install pymongo
from functools import partial
from sys import maxint
import sys
from TwitterRequest import make_twitter_request
import datetime
import time
from datetime import datetime as date
from Twitter import oauth_login



database_followers = 'twitter_followers'
database_reports = 'twitter_followers_reports'

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
            coll.insert({"_id":value,"fetch_time":datetime.datetime.utcnow()})
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
        if data[key] == None:
            print >> sys.stderr, 'twitter user id not valid ',key,json.dumps(data[key], indent=4)
        try:
            collection.update({"_id":key},{"$set":data[key]} , upsert=True)
        except pymongo.errors.DuplicateKeyError:
            print >> sys.stderr, 'duplicate twitter user id ', key
        except pymongo.errors.PyMongoError as e:
            print >> sys.stderr, 'Failed to save follower id ', key, e
    
    client.disconnect()    
    

def get_follower_profiles(screen_name,app_user,twitter_oauth,idsOnly = False):
    
    collection = '{0}_{1}'.format(app_user, screen_name)
    database = 'twitter_followers'
     
    # step 1: collect all follower ids
    print >> sys.stderr, 'Getting follower ids for {0}'.format(collection)
    result = followers_ids_to_mongodb(collection, twitter_oauth, screen_name=screen_name,
                                friends_limit=0, database='twitter_followers')    
    print >> sys.stderr, 'Saved {0} follower ids'.format(result)
     
    if not idsOnly:
        # step 2: get follower info
        print >> sys.stderr, 'Getting follower profile info for {0}'.format(collection)
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
    
def generate_report(target_users,compare_to_users,app_user,**mongo_conn_kw):
    
    
    # create report collection
    client = pymongo.MongoClient(**mongo_conn_kw)
    
    #init collections 
    target_user_coll = client[database_followers]['{0}_{1}'.format(app_user, target_users)]
    reports_coll = client[database_reports]['reports']
         
    info = {'app_user':app_user,'target_users':target_users,'compare_to_users':compare_to_users, 'report_date':datetime.datetime.utcnow()}
    
    compare_to_user_cursor = get_followers_from_mongo(database_followers, '{0}_{1}'.format(app_user, compare_to_users))
    for item in compare_to_user_cursor:
        
        try:
            # follower info
            d = date.strptime(item["created_at"],'%a %b %d %H:%M:%S +0000 %Y')
            report_row = {"info":info,
                          "name":item["name"] ,
                          "screen_name":item["screen_name"],
                          "followers_count":item["followers_count"],
                          "friends_count":item["friends_count"],
                          "statuses_count":item["statuses_count"],
                          "created_at": d.strftime('%Y-%m-%d'),
                          "verified":item["verified"]   }
            if  target_user_coll.find_one({"_id": item['_id']}):
                report_row["follows_"+target_users]=True
            else:
                report_row["follows_"+target_users]=False
            
            #TODO check if target user follows the compare to user (friends list) 
            reports_coll.insert(report_row)
            
        except Exception as e:
            print >> sys.stderr, 'Failed to compared for user {0} and follower Id {1}'.format(compare_to_users,item)
        
          
    compare_to_user_cursor.close()     
    client.disconnect() 
    print >> sys.stderr, 'Generated report for Target users: {0} , Compare to users:{1}'.format(target_users,compare_to_users)   

def follower_data_expired(app_user,target_users,timedelta=0,**mongo_conn_kw):
    # Get a reference to a particular database
    try:
        client = pymongo.MongoClient(**mongo_conn_kw)
        collection = client[database_followers]['{0}_{1}'.format(app_user, target_users)]
        cursor = collection.find_one(timeout=False)

        delta = datetime.datetime.utcnow() - cursor["fetch_time"]
        hours = delta.total_seconds()/3600
        print >> sys.stderr, 'Follower data age is {0} hours old for {1}'.format(hours,'{0}_{1}'.format(app_user, target_users))
        if hours > timedelta:
            return True
        else:
            return False
    except Exception:
        print >> sys.stderr, 'Failed to check for follower data age for {0}'.format('{0}_{1}'.format(app_user, target_users))
        print >> sys.stderr, 'Error type ',sys.exc_info()
        return True

def compare_workflow(target_users,compare_to_users,app_user,twitter_oauth):
    
    
    # get twitter user's followers info 
    # check if recent data exists
    if follower_data_expired(app_user,target_users,timedelta=0):
        start_time = time.time()
        get_follower_profiles(target_users,app_user,twitter_oauth,idsOnly = True)
        print "Time getting follower id: ", (time.time() - start_time)/60, " minutes"
     
    # get compare to users
    # we always get renew this data
    start_time = time.time()    
    get_follower_profiles(compare_to_users,app_user,twitter_oauth)
    print "Time getting follower profiles: ", (time.time() - start_time)/60, " minutes"
    
    start_time = time.time()
    generate_report(target_users,compare_to_users,app_user)
    print "Time generating report: ", (time.time() - start_time)/60, " minutes"


if __name__ == '__main__':
    
    # Sample usage
    target_users = 'mrdrewscott'
    compare_to_users = 'eventpeeks'
    app_user = 'bart'
    
    # twitter user tokens
    twitter_oauth = oauth_login() 

    compare_workflow(target_users.lower(),compare_to_users.lower(),app_user.lower(),twitter_oauth)
    
    
#################################
# Sample follower info document in mongodb
#################################
# {
#     "_id" : NumberLong("3092915712"),
#     "follow_request_sent" : false,
#     "profile_use_background_image" : true,
#     "profile_text_color" : "333333",
#     "default_profile_image" : true,
#     "id" : NumberLong("3092915712"),
#     "profile_background_image_url_https" : "https://abs.twimg.com/images/themes/theme1/bg.png",
#     "verified" : false,
#     "profile_location" : null,
#     "profile_image_url_https" : "https://abs.twimg.com/sticky/default_profile_images/default_profile_5_normal.png",
#     "profile_sidebar_fill_color" : "DDEEF6",
#     "entities" : {
#         "description" : {
#             "urls" : [ ]
#         }
#     },
#     "followers_count" : 11,
#     "profile_sidebar_border_color" : "C0DEED",
#     "id_str" : "3092915712",
#     "profile_background_color" : "C0DEED",
#     "listed_count" : 0,
#     "is_translation_enabled" : false,
#     "utc_offset" : null,
#     "statuses_count" : 0,
#     "description" : "",
#     "friends_count" : 331,
#     "location" : "",
#     "profile_link_color" : "0084B4",
#     "profile_image_url" : "http://abs.twimg.com/sticky/default_profile_images/default_profile_5_normal.png",
#     "following" : false,
#     "geo_enabled" : false,
#     "profile_background_image_url" : "http://abs.twimg.com/images/themes/theme1/bg.png",
#     "name" : "Hahxuax",
#     "lang" : "en",
#     "profile_background_tile" : false,
#     "favourites_count" : 0,
#     "screen_name" : "hahxuax",
#     "notifications" : false,
#     "url" : null,
#     "created_at" : "Tue Mar 17 09:01:34 +0000 2015",
#     "contributors_enabled" : false,
#     "time_zone" : null,
#     "protected" : false,
#     "default_profile" : true,
#     "is_translator" : false
# }




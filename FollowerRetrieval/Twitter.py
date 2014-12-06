from twitter import *

def oauth_login():
    # XXX: Go to http://twitter.com/apps/new to create an app and get values
    # for these credentials that you'll need to provide in place of these
    # empty string values that are defined as placeholders.
    # See https://dev.twitter.com/docs/auth/oauth for more information 
    # on Twitter's OAuth implementation.
    
    
    
    CONSUMER_KEY = 'xtyAQ1HvaPXHa72oLZulukvPh'
    CONSUMER_SECRET = 'IVgGl2JZdLjMqHo96zCOHleaTNUtexKpuNh82pb7NGIwC7bNyw'
    OAUTH_TOKEN = '2381523546-yHqNJ50ttgIZ2fIpzzZWUI1V0QVk6GM0i5i0lr8'
    OAUTH_TOKEN_SECRET = 'yCejVavhbkjrHcqfpnW85KJkjc6cSdtk9zkSUwdvTgsHX'
    
    return Twitter(auth=OAuth(OAUTH_TOKEN,OAUTH_TOKEN_SECRET,CONSUMER_KEY, CONSUMER_SECRET))

#!/usr/bin/env python

# Copyright KOLIBERO under one or more contributor license agreements.  
# KOLIBERO licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import uuid
import argparse
from datetime import datetime

# Twitter libs
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

def save_tweet(data):
  print "save_tweet"
  print data

class StdOutListener(StreamListener):
    def on_data(self, data):
      try:
          message = json.loads(data)
          if args.show_raw=="yes":
            print message
          text = message.get("text").encode("utf8")
          screen_name = message.get("user").get("screen_name")
          source = message.get("source")
          id = message.get("id")
          created_at = message.get("created_at")
          location = message.get("user").get("location")
          timestamp_ms = message.get("timestamp_ms")
          lang = message.get("lang")
          user_id = message.get("user").get("id")
          print "Text:\n",text
          print "Screen Name:",screen_name
          print "Source:",source
          print "ID:",id
          print "Timestamp [ms]:",timestamp_ms
          print "Created At:",created_at
          print "Location:",location
          print "lang:",lang
          print "user_id:",user_id
          print "-----------------------"
          #
          if args.save == "yes":
            save_tweet({"user_id":user_id,"text":text,"screen_name":screen_name,"source":source,"id":id,"created_at":created_at,"location":location,"timestamp_ms":timestamp_ms,"lang":lang})
      except Exception, Argument:
          print "Unexpected Error!", Argument
          print(data)
      return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Snowplow Twitter Tracker")
  parser.add_argument('--access_token', default="")
  parser.add_argument('--access_token_secret', default="")
  parser.add_argument('--consumer_key', default="")
  parser.add_argument('--consumer_secret', default="")
  parser.add_argument('--follow', default="2193645284,721938046472581120,799541035466637312")
  parser.add_argument('--show_raw', default="no")
  parser.add_argument('--save', default="no")

  args = parser.parse_args()
  print "access_token:",args.access_token
  print "access_token_secret:",args.access_token_secret
  print "consumer_key:",args.consumer_key
  print "consumer_secret:",args.consumer_secret
  print "follow:",args.follow
  print "show_raw:",args.show_raw
  print "save:",args.save
  print "--"

  print "Starting listening to Twitter..."

  l = StdOutListener()
  auth = OAuthHandler(args.consumer_key, args.consumer_secret)
  auth.set_access_token(args.access_token, args.access_token_secret)
  stream = Stream(auth, l)

  follow_arr = args.follow.split(",")
  print "follow_arr:",follow_arr

  stream.filter(follow=follow_arr)




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
import time
import base64
from datetime import datetime
from random import randint
import logging

# Twitter libs
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Snowplow libs
from snowplow_tracker import Subject, Emitter, Tracker, SelfDescribingJson

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

global emiter
global tracker
global period_counter
global aggregates

def save_tweet(data):
  global tracker
  print "save_tweet:",__name__
  #print data
  

  indata = data

  s1 = Subject()
  s1.set_platform("web")
  s1.set_user_id(str(indata.get("user_id")))
  s1.set_lang(str(indata.get("lang")))
  #s1.set_ip_address(str(indata.get("i_ip")))
  s1.set_useragent(str(indata.get("source")))

  tracker.set_subject(s1)

  tracker.track_self_describing_event(SelfDescribingJson("iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",{
      "data":{
        "data": indata
      },
      "schema": "iglu:com.rbox24/"+args.sp_app_id+"/jsonschema/1-0-0"
  }))

  #tracker.flush()
  print "Tweet sent to collector, time:",time.time()

class StdOutListener(StreamListener):
    def on_data(self, data):
      global aggregates
      global period_counter
      payload = None
      try:
          message = json.loads(data)
          if args.show_raw=="yes":
            print message
          screen_name = message.get("user").get("screen_name")
          if screen_name:
            screen_name = screen_name.encode("utf8","ignore")
          location = message.get("user").get("location")
          if location:
            location = location.encode("ascii","ignore")
          lang = message.get("lang")
          if lang:
            lang = lang.encode("utf8")
          #print "-----------------------"
          #
          # Aggregate
          key = str(screen_name)+str(lang)+str(location)
          payload = aggregates.get(key)
          if not payload:
            aggregates[key] = {"follow":args.follow,"track":"None","timestamp":int(time.time())*1000,"count":1, "screen_name":screen_name, "location":str(location), "lang":str(lang)}
          else:
            count = payload.get("count")
            count += 1
            payload["count"] = count
            payload["timestamp"] = int(time.time()*1000)
            aggregates[key] = payload

          #print period_counter+int(args.period),int(time.time())
          #for i in aggregates.items():
          #  print i[1]
          # Save
          if period_counter+int(args.period) < int(time.time()+randint(0,60)):
            period_counter = int(time.time()) 
            if args.save == "yes":
              for i in aggregates.items():  
                #print i
                save_tweet(i[1])
            aggregates = {}    
      except Exception, Argument:
          print "Unexpected Error!", Argument
          if str(Argument).find("'ascii' codec can't encode character") == -1:
            raise
          #print data
          #print payload
      return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Snowplow Twitter Tracker")
  parser.add_argument('--access_token', default="")
  parser.add_argument('--access_token_secret', default="")
  parser.add_argument('--consumer_key', default="")
  parser.add_argument('--consumer_secret', default="")
  parser.add_argument('--follow', default="2193645284")
  parser.add_argument('--show_raw', default="no")
  parser.add_argument('--save', default="no")
  parser.add_argument('--period', default="600")
  parser.add_argument('--sp_collector_uri', default="example.com")
  parser.add_argument('--sp_collector_protocol', default="https")
  parser.add_argument('--sp_collector_port', default="444")
  parser.add_argument('--sp_collector_method', default="post")
  parser.add_argument('--sp_app_id', default="snowplow_twitter_aggregator")

  args = parser.parse_args()
  print "access_token:",args.access_token
  print "access_token_secret:",args.access_token_secret
  print "consumer_key:",args.consumer_key
  print "consumer_secret:",args.consumer_secret
  print "follow:",args.follow
  print "show_raw:",args.show_raw
  print "save:",args.save
  print "period:",args.period
  print "sp_collector_uri:",args.sp_collector_uri
  print "sp_collector_protocol:",args.sp_collector_protocol
  print "sp_collector_port:",args.sp_collector_port
  print "sp_collector_method:",args.sp_collector_method
  print "sp_app_id:",args.sp_app_id
  print "--"

  print "Tracker setup..."
  #global emiter
  #global tracker
  emiter = Emitter(buffer_size=50, endpoint=args.sp_collector_uri, protocol=args.sp_collector_protocol, port=int(args.sp_collector_port), method=args.sp_collector_method)
  tracker = Tracker(emitters=emiter,namespace="cf",app_id=args.sp_app_id,encode_base64=True)

  print "Starting listening to Twitter..."

  #global period_counter
  aggregates = {}
  period_counter = int(time.time())

  l = StdOutListener()
  auth = OAuthHandler(args.consumer_key, args.consumer_secret)
  auth.set_access_token(args.access_token, args.access_token_secret)
  stream = Stream(auth, l)

  follow_arr = args.follow.split(",")
  print "follow_arr:",follow_arr

  stream.filter(follow=follow_arr)

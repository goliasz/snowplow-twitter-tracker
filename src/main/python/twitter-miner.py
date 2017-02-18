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
from datetime import datetime

# Twitter libs
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Snowplow libs
from snowplow_tracker import Subject, Emitter, Tracker, SelfDescribingJson

def save_tweet(data):
  #print "save_tweet"
  #print data

  indata = data

  e = Emitter(args.sp_collector_uri,protocol=args.sp_collector_protocol,port=int(args.sp_collector_port),method=args.sp_collector_method)
  t = Tracker(emitters=e,namespace="cf",app_id=args.sp_app_id,encode_base64=True)

  s1 = Subject()
  s1.set_platform("web")
  s1.set_user_id(str(indata.get("user_id")))
  s1.set_lang(str(indata.get("lang")))
  #s1.set_ip_address(str(indata.get("i_ip")))
  s1.set_useragent(str(indata.get("source")))

  t.set_subject(s1)

  t.track_self_describing_event(SelfDescribingJson("iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",{
      "data":{
        "data": indata
      },
      "schema": "iglu:com.rbox24/"+args.sp_app_id+"/jsonschema/1-0-0"
  }))

  t.flush()
  print "Tweet sent to collector, time:",time.time()

class StdOutListener(StreamListener):
    def on_data(self, data):
      payload = None
      try:
          message = json.loads(data)
          if args.show_raw=="yes":
            print message
          text = message.get("text")
          if text:
            text = text.encode("ascii","ignore")
          screen_name = message.get("user").get("screen_name")
          if screen_name:
            screen_name = screen_name.encode("utf8","ignore")
          source = message.get("source")
          if source:
            source = source.encode("ascii","ignore")
          id = message.get("id")
          created_at = message.get("created_at")
          location = message.get("user").get("location")
          if location:
            location = location.encode("ascii","ignore")
          timestamp_ms = message.get("timestamp_ms")
          lang = message.get("lang")
          if lang:
            lang = lang.encode("utf8")
          user_id = message.get("user").get("id")
          """
          print "Text:\n",text
          print "Screen Name:",screen_name
          print "Source:",source
          print "ID:",id
          print "Timestamp [ms]:",timestamp_ms
          print "Created At:",created_at
          print "Location:",location
          print "lang:",lang
          print "user_id:",user_id
          """
          print "-----------------------"
          #
          if args.save == "yes":
            payload = {"user_id":user_id,
                       "text":text,
                       "screen_name":screen_name,
                       "source":source,
                       "id":id,
                       "created_at":created_at,            
                       "location":location,
                       "timestamp_ms":timestamp_ms,
                       "lang":lang}
            if args.restrict_source == "yes":
              follow_arr0 = args.follow.split(",")
              if user_id in follow_arr0:
                save_tweet(payload)
            else:
              save_tweet(payload)
      except Exception, Argument:
          print "Unexpected Error!", Argument
          print data
          print payload
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
  parser.add_argument('--restrict_source', default="no")
  parser.add_argument('--sp_collector_uri', default="example.com")
  parser.add_argument('--sp_collector_protocol', default="https")
  parser.add_argument('--sp_collector_port', default="444")
  parser.add_argument('--sp_collector_method', default="post")
  parser.add_argument('--sp_app_id', default="snowplow_twitter_tracker")

  args = parser.parse_args()
  print "access_token:",args.access_token
  print "access_token_secret:",args.access_token_secret
  print "consumer_key:",args.consumer_key
  print "consumer_secret:",args.consumer_secret
  print "follow:",args.follow
  print "show_raw:",args.show_raw
  print "save:",args.save
  print "restrict_source:",args.restrict_source
  print "sp_collector_uri:",args.sp_collector_uri
  print "sp_collector_protocol:",args.sp_collector_protocol
  print "sp_collector_port:",args.sp_collector_port
  print "sp_collector_method:",args.sp_collector_method
  print "sp_app_id:",args.sp_app_id
  print "--"

  print "Starting listening to Twitter..."

  l = StdOutListener()
  auth = OAuthHandler(args.consumer_key, args.consumer_secret)
  auth.set_access_token(args.access_token, args.access_token_secret)
  stream = Stream(auth, l)

  follow_arr = args.follow.split(",")
  print "follow_arr:",follow_arr

  stream.filter(follow=follow_arr)




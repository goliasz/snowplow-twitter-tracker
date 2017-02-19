# snowplow-twitter-tracker

## Installation
```
$ sudo apt-get install python-dev
$ sudo pip install tweepy
$ sudo pip install snowplow-tracker
$ sudo pip install contracts
$ git clone https://github.com/goliasz/snowplow-twitter-tracker.git
```
## Twitter Setup

1. Create your app here https://apps.twitter.com/
2. Generate your access token

## Start
```
$ export TWITTER_TOKEN="<HERE TOKEN>" 
$ export TWITTER_TOKEN_SECRET="<HERE TOKEN SECRET>" 
$ export TWITTER_KEY="<HERE KEY>" 
$ export TWITTER_SECRET="<HERE KEY SECRET>"
$ export SP_COLLECTOR_URI="<HERE COLLECTOR URI>"
$ export SP_COLLECTOR_PORT=443
$ export SP_COLLECTOR_PROTOCOL="https"
$ export SP_COLLECTOR_METHOD="post"
$ cd snowplow-twitter-tracker
$ sh src/main/script/twitter-miner.sh "1367531,14293310" "yes" "no"
```

# License
Apache License, Version 2.0

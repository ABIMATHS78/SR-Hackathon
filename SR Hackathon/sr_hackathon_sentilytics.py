import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from datetime import datetime
from unidecode import unidecode
import calendar
import re
import numpy as np
from http.client import IncompleteRead
from urllib3.exceptions import ProtocolError
from queue import Queue
from threading import Thread #multithreading

# from urllib3.exceptions import ProtocolError

consumer_key = "ePAWETme6ofxq9in6jHSOaIh5"
consumer_secret = "tP5594vdPLyzRACxMfPBkY8VEBVRRvVOvWXEGNyz17FoVml8xl"
access_token = "975454118390231040-3qJN1xwPf31ph8rzujtQsYipa5HFiwB"
access_token_secret = "DkLVGFT3Ucw4JuHaVz09GUByeTHu66Gjp0lcdvAXcxQFv"

# create instance of elasticsearch
es = Elasticsearch()

analyzer = SentimentIntensityAnalyzer()

#Remove URL from tweets
def remove_url(txt):
    """Replace URLs found in a text string with nothing
    (i.e. it will remove the URL from the string).

    Parameters
    ----------
    txt : string
        A text string that we want to parse and remove urls.

    Returns
    -------
    The same txt string with url's removed.
    """

    return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())

tracks = ["AppleInc", "KelloggCompany", "VisaInc", "WellsFargoandCompany", "BankOfAmerica", "UnitedAirlinesHoldings", "FordMotorCo", "ExxonMobil", "Chevron", "JohnsonandJohnson", "Amazon", "Abbvie", "Biogen", "Fedex", "Hershey", "Tesla"]

class TweetStreamListener(StreamListener):
    def __init__(self, q= Queue()):
        super().__init__()
        self.q = q
        for i in range(20):
            t=Thread(target=self.do_stuff)
            t.daemon = True
            t.start()


    def on_data(self, data):
        self.q.put(data)
        # To understand the key-values pulled from Twitter, see 'https://dev.twitter.com/overview/api/tweets'
        dict_data = json.loads(data)

        if 'truncated' not in dict_data:
            print(data)
            return True
        if dict_data['truncated']:
            tweet = unidecode(dict_data['extended_tweet']['full_text'])
        else:
            tweet = unidecode(dict_data['text'])

        tweet = remove_url(tweet)
        vs = analyzer.polarity_scores(tweet)
        score = vs['compound']
        if score:
            # determine if sentiment is positive, negative, or neutral
            if score <= -0.05:
                sentiment = "negative"
            elif score >= 0.05:
                sentiment = "positive"
            elif -0.05 < score < 0.05:
                sentiment = "neutral"
            else:
                sentiment = " "
            # print the predicted sentiment with the Tweets
            print(tweet, score, sentiment)


            # extract the first hashtag from the object
            # transform the Hashtags into proper case
            if len(dict_data["entities"]["hashtags"])>0:
                hashtags=dict_data["entities"]["hashtags"][0]["text"].title()
            else:
                #Elasticeach does not take None object
                hashtags="None"

            try:
                val1 = int(dict_data["user"]["followers_count"])
            except:
                val1 = 0
            try:
                val2 = int(dict_data["retweet_count"])
            except:
                val2 = 0
            try:
                val3 = int(dict_data["favorite_count"])
            except:
                val3 = 0
            # add text and sentiment info to elasticsearch

            #Classifying the tweets into organization and industry


            # List of sectors that we are trackering
            fs = "Financial"
            tech = "Technology"
            cnc = "Consumer Non Cyclical"
            trpt = "Transportation"
            cdsp = "Consumer Discretionary Sector Performance"
            enrg = "Energy"
            hltc = "Health Care"
            rtl = "Retail"



            org_dict = {
                "AppleInc".lower():["Apple Inc", tech, "Computer Hardware"],
                "KelloggCompany".lower():["Kellogg Company", cnc, "Food Processing"],
                "VisaInc".lower():["Visa Inc", fs, "Consumer Financial Services"],
                "WellsFargoandCompany".lower():["Wells Fargo and Company", fs, "Money Center Banks"],
                "BankOfAmerica".lower():["Bank Of America Corporation", fs, "Money Center Banks"],
                "UnitedAirlinesHoldings".lower():["United Airlines Holdings Inc", trpt, "Airline"],
                "FordMotorCo".lower():["Ford Motor Co", cdsp, "Auto & Truck Manufacturers"],
                "ExxonMobil".lower():["Exxon Mobil Corporation", enrg, "Oil & Gas Integrated Operations"],
                "Chevron".lower():["Chevron Corp", enrg, "Oil & Gas Integrated Operations"],
                "JohnsonandJohnson".lower():["Johnson and Johnson", hltc, "Major Pharmaceutical Preparations"],
                "Amazon".lower():["Amazon Com Inc", rtl, "Internet, Mail Order & Online Shops"],
                "Abbvie".lower():["Abbvie inc", hltc, "Biotechnology & Drugs"],
                "Biogen".lower():["Biogen Inc", hltc, "Biotechnology & Drugs"],
                "Fedex".lower():["Fedex Corporation", trpt, "Transport & Logistics"],
                "Hershey".lower():["Hershey Co", cnc, "Food Processing"],
                "Tesla".lower():["Tesla Inc", cdsp, "Auto & Truck Manufacturers"]
            }

            #finacial services
            dict_data['organization'] = None
            hash_mod = None
            reply_name = None
            
            if hashtags == None:
                hash_mod = ""
            else:
                hash_mod = str(hashtags)
            
            if not dict_data["in_reply_to_screen_name"]:
                reply_name = str(dict_data["in_reply_to_screen_name"])
            else:
                reply_name = ""
            
            for tracker in tracks:
                # if dict_data.get('text') and tracker in dict_data['text']:
                tracker = tracker.lower()
                if tracker in dict_data['text'].lower() or tracker in hash_mod.lower() or tracker in reply_name.lower():
                    # print(True, tracker)
                    dict_data['organization']= org_dict[tracker][0]
                    dict_data['sector'] = org_dict[tracker][1]
                    dict_data['sub-sector'] = org_dict[tracker][2]
                    break
            if not dict_data['organization']:
                dict_data['organization'] = "Others"
                dict_data['sector'] = "Others"
                dict_data['sub-sector'] = "Others"

            # print(dict_data["in_reply_to_screen_name"], hashtags)
            # print("-------------------------------------------------------------------------------", dict_data['sector'], dict_data['organization'], hashtags, dict_data["user"]["screen_name"])

            body_hold={
                "sentiment": sentiment,
                "location": dict_data["user"]["location"],
                "retweet": dict_data["retweet_count"],
                "score": score,
                "hashtags": hashtags,
                #"message": dict_data["text"]  if "text" in dict_data.keys() else " ",
                "message": tweet,
                #parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
               "date": datetime.strptime(dict_data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
               "followers":dict_data["user"]["followers_count"],
               "likes":dict_data["favorite_count"],
               "engagement_score":str(val1+val2+val3),
               "author": dict_data["user"]["screen_name"],
               "organization":dict_data['organization'],
               "sector":dict_data['sector'],
               "sub-sector":dict_data['sub-sector'],
               }
            
            print("------------------------------------", body_hold)
            es.index(index="sr_hackathon_main",
                     # create/inject data into the cluster with index as 'logstash-a'
                     # create the naming pattern in Management/Kibana later in order to push the data to a dashboard
                     doc_type="twitter_sentiment",
                     body={
                            "sentiment": sentiment,
                            "location": dict_data["user"]["location"],
                            "retweet": dict_data["retweet_count"],
                            "score": score,
                            "hashtags": hashtags,
                            #"message": dict_data["text"]  if "text" in dict_data.keys() else " ",
                            "message": tweet,
                            #parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                           "date": datetime.strptime(dict_data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                           "followers":dict_data["user"]["followers_count"],
                           "likes":dict_data["favorite_count"],
                           "engagement_score":str(val1+val2+val3),
                           "author": dict_data["user"]["screen_name"],
                           "organization":dict_data['organization'],
                           "sector":dict_data['sector'],
                           "sub-sector":dict_data['sub-sector'],
                           })
        return True



    def do_stuff(self):
        while True:
            self.q.get()
            self.q.task_done()

    # on failure, print the error code and do not disconnect
    def on_error(self, status):
        print(status)
        return

    def on_exception(self,exception):
        print(exception)
        return

if __name__ == '__main__':
    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # The most exception break up the kernel in my test is ImcompleteRead. This exception handler ensures
    # the stream to resume when breaking up by ImcompleteRead

    while True:
        try:
            # create instance of the tweepy stream
            stream = Stream(auth, listener)
            # search twitter for keyword "trump"
            stream.filter(track= tracks, stall_warnings=True,languages=["en"])

            print(stream)

        except (ProtocolError,AttributeError):
            continue
        except KeyboardInterrupt:
            # or however you want to exit this loop
            stream.disconnect()
            break






"""
RUNNING PROGRAM;

1-Start Apache Kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

2-Run kafka_push_listener.py (Start Producer)
PYSPARK_PYTHON=python3 bin/spark-submit kafka_push_listener.py

3-Run kafka_twitter_spark_streaming.py (Start Consumer)
PYSPARK_PYTHON=python3 bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 kafka_twitter_spark_streaming.py
"""
from __future__ import division
from collections import Counter
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql import SQLContext
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
from googletrans import Translator
from datetime import datetime
from pyspark import sql
import json
import re
import nltk
import urllib.request as urllib2
import numpy as np
import pickle
import csv
import os
from pyspark.sql.types import Row
vector = {    'depth_retweets':0,
              'ratio_retweets':0,
              'hashtags':0,
              'length':0,
              'exclamations':0,
              'questions':0,
              'links':0,
              'topicRepetition':0,
              'replies':0,
              'spreadVelocity':1,
              'user_diversity':0,
              'retweeted_user_diversity':0,
              'hashtag_diversity':0,
              'language_diversity':0,
              'vocabulary_diversity':0}
t0_original = ''
data = {}
data_tweet_geo = {}
removal_list = ['\\','/',',','(',')','!',':','.']
stop = stopwords.words('english')

translator = Translator()
def getIntent(x):
    tknzr = TweetTokenizer(reduce_len=True)
    a = tknzr.tokenize(x)
    # # # ##print "Twitter tokens: ", a
    tagged = nltk.pos_tag(a)
    # # # ##print 'Tagged: ', tagged

    y = []
    for x in tagged:
        if x[1] == 'VB':
            return x[0]
        else:
            return 'No Verb Found'
outFile = open('temp1.txt',"w")

# writer.writerow(['depth_retweets','ratio_retweets','hashtags',
#         'length','exclamations','questions',
#         'links','topicRepetition','replies',
#         'spreadVelocity','user_diversity1',
#         'retweeted_user_diversity1','hashtag_diversity1',
#         'language_diversity1','vocabulary_diversity1'])
def saveDataToFile(x):
        x = x.collect()
        #print('xzxzxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')
        #print(x)
        for data in x:
            outFile.write(data)
        outFile.write('\n')
            # writer.writerow([data[0],data[1]])

def turnIntoVector(x):
    #x = x.collect()
    dataFrame = x.toDF()

def getRDD(x):
    if(x!= None):
        return x

def isReplies(a):
    if a and a>0:
        return 1
    else:
        return 0

numberItem = 0
def increaseBag(key, bag):
    try:
        ##print("-------xxxxxxxxxxxxxx------------" + str(key))
        ##print(bag)
        if str(key) in bag.keys():
            bag[key]+=1
        else:
            bag[key]=1
    except:
        pass


def calShannon(bag):
    sum=0
    for item in bag:
        temp = bag[item]/ numberItem
        sum += temp*np.log(temp)
    return -1*sum

#Average number of retweet levels in tweets.
depth_retweets=0
#Ratio of tweets that contain a retweet.
ratio_retweets=0
#Average number of hashtags in tweets.
hashtags=0
#Average length of tweets
length=0
#Number of tweets with exclamation signs.
exclamations=0
#Number of question signs in tweets.
questions=0
#Average number of links in tweets
links=0
#Average number of uses of the trending topic in tweets.
topicRepetition=0
#Average number of tweets that are replies to others
replies=0
#Average number of tweets per second in the trend
spreadVelocity=0

user_diversity=dict()

retweeted_user_diversity=dict()

hashtag_diversity=dict()

language_diversity=dict()

vocabulary_diversity=dict()
timeStart = datetime.now()
timeEnd = datetime.now()

def checkExistFile(filepath):
    if  not os.path.isfile(filepath):
        f= open(filepath,"w+")
        f.close()

def loadDataFromFile():
    global timeStart
    global timeEnd
    global numberItem
    global depth_retweets
    global ratio_retweets
    global hashtags
    global length
    global exclamations
    global questions
    global links
    global topicRepetition
    global replies
    global spreadVelocity
    global user_diversity
    global retweeted_user_diversity
    global hashtag_diversity
    global language_diversity
    global vocabulary_diversity
    checkExistFile("data.txt")
    checkExistFile("user_diversity.txt")
    checkExistFile("retweeted_user_diversity.txt")
    checkExistFile("hashtag_diversity.txt")
    checkExistFile("language_diversity.txt")
    checkExistFile("vocabulary_diversity.txt")
    f = open ("data.txt", "r")
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx')
    print(f.readlines()[0] + 'wtf')
    f = open ("data.txt", "r")
    numberItem = int(f.readlines()[0])
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx')
    print(numberItem)
    f = open ("data.txt", "r")
    ratio_retweets = float(f.readlines()[1])
    f = open ("data.txt", "r")
    hashtags = float(f.readlines()[2])
    f = open ("data.txt", "r")
    length = float(f.readlines()[3])
    f = open ("data.txt", "r")
    exclamations = float(f.readlines()[4])
    f = open ("data.txt", "r")
    questions = float(f.readlines()[5])
    f = open ("data.txt", "r")
    links = float(f.readlines()[6])
    f = open ("data.txt", "r")
    topicRepetition = float(f.readlines()[7])
    f = open ("data.txt", "r")
    replies = float(f.readlines()[8])
    f = open ("data.txt", "r")
    tempTime = f.readlines()[9].replace("\n","")
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    # print(tempTime)
    if(len(tempTime) > 23):
        timeStart = datetime.strptime(tempTime,'%Y-%m-%d %H:%M:%S.%f')
    else:
        print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
        print(tempTime)
        timeStart = datetime.strptime(tempTime,'%Y-%m-%d %H:%M:%S')
    f = open ("data.txt", "r")
    tempTime = f.readlines()[10].replace("\n","")
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    # print(tempTime)
    if(len(tempTime) > 23):
        timeStart = datetime.strptime(tempTime,'%Y-%m-%d %H:%M:%S.%f')
    else:

        timeStart = datetime.strptime(tempTime,'%Y-%m-%d %H:%M:%S')

    f.close()
    try:
        user_diversity  = json.load(open("user_diversity.txt"))
        retweeted_user_diversity  = json.load(open("retweeted_user_diversity.txt"))
        hashtag_diversity  = json.load(open("hashtag_diversity.txt"))
        language_diversity  = json.load(open("language_diversity.txt"))
        vocabulary_diversity  = json.load(open("vocabulary_diversity.txt"))
    except:
        pass




def saveDataToFile():
    f = open('data.txt', 'w+')
    f.truncate()
    f.write(str(numberItem))
    f.write("\n")
    f.write(str(ratio_retweets))
    f.write("\n")
    f.write(str(hashtags))
    f.write("\n")
    f.write(str(length))
    f.write("\n")
    f.write(str(exclamations))
    f.write("\n")
    f.write(str(questions))
    f.write("\n")
    f.write(str(links))
    f.write("\n")
    f.write(str(topicRepetition))
    f.write("\n")
    f.write(str(replies))
    f.write("\n")
    f.write(str(timeStart))
    f.write("\n")
    f.write(str(timeEnd))
    f.close()
    f = open('user_diversity.txt', 'w+')
    f.truncate()
    f.close()
    f = open('retweeted_user_diversity.txt', 'w+')
    f.truncate()
    f.close()
    f = open('language_diversity.txt', 'w+')
    f.truncate()
    f.close()
    f = open('vocabulary_diversity.txt', 'w+')
    f.truncate()
    f.close()
    f = open('hashtag_diversity.txt', 'w+')
    f.truncate()
    f.close()
    json.dump(user_diversity, open("user_diversity.txt",'w'))
    json.dump(retweeted_user_diversity, open("retweeted_user_diversity.txt",'w'))
    json.dump(hashtag_diversity, open("hashtag_diversity.txt",'w'))
    json.dump(language_diversity, open("language_diversity.txt",'w'))
    json.dump(vocabulary_diversity, open("vocabulary_diversity.txt",'w'))


def getFeature(x):
    global numberItem
    loadDataFromFile()
    numberItem = numberItem + 1

    #print ('------------' + json.dumps(x, indent = 4) + '----------------')
    res = json.loads(json.dumps(x, indent = 4))
    print('asddddddddddddddddddadsada',res['text'])
    #print(res)
    test = {
        'userId':res['user']['id'],
        'tweet':res['text'],
        'retweet_count':res['retweet_count'],
        'arr_hashtags':res['entities']['hashtags'],
        'links':len(res['entities']['urls']),
        'isReplies':isReplies(res['in_reply_to_status_id']),
        'created':res['created_at'],
        'lang':res['lang']
    }
    ###print('------------' + json.dumps(test, indent = 4) + '------------')
    #Return feature
     # 2
    tweetJson = test
    ###print(json.dumps(test, indent = 4))
    global timeEnd
    global timeStart
    global depth_retweets
    global ratio_retweets
    global hashtags
    global length
    global exclamations
    global questions
    global links
    global topicRepetition
    global replies
    global spreadVelocity
    global user_diversity
    global retweeted_user_diversity
    global hashtag_diversity
    global language_diversity
    global vocabulary_diversity
    ###print (tweetJson)
    topicName = 'Rich Eisen'.lower()
    #print('--------------------------------1-----' + str(tweetJson['retweet_count']))
    #print('--------------------------------2-----' + str(tweetJson['arr_hashtags']))
    #print('--------------------------------3-----' + str(tweetJson['tweet']))
    #print('--------------------------------4-----' + str(tweetJson['links']))
    #print('--------------------------------5-----' + str(tweetJson['isReplies']))
    #print('--------------------------------6-----' + str(tweetJson['userId']))
    #print('--------------------------------7-----' + str(numberItem))
    ##print(tweetJson)
    ##print('--------------------------------1-----' + str(tweetJson['text']))
    if tweetJson['retweet_count']>0 :
        ##print('--------------------------------1-----' + str(depth_retweets))
        depth_retweets=depth_retweets+1
        ##print('--------------------------------1-----' + str(depth_retweets))
    if tweetJson['retweet_count']>0 :
        ##print('--------------------------------2-----' + str(ratio_retweets))
        ratio_retweets=ratio_retweets+1
        ##print('--------------------------------2-----' + str(ratio_retweets))
    hashtags+=len(tweetJson['arr_hashtags'])
    length+=len(tweetJson['tweet'])
    if '!' in tweetJson['tweet']:
        ##print('--------------------------------3-----' + str(exclamations))
        exclamations=exclamations+1
        ##print('--------------------------------3-----' + str(exclamations))
    if '?' in tweetJson['tweet']:
        ##print('--------------------------------4-----' + str(questions))
        questions=questions+1
        ##print('--------------------------------4-----' + str(questions))
    links+=tweetJson['links']
    ##print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"+ str(tweetJson['userId']))
    topicRepetition += tweetJson['tweet'].lower().count(topicName)
    if tweetJson['isReplies']>0:
        replies+=1
    increaseBag(tweetJson['userId'], user_diversity)
    if tweetJson['retweet_count']>0:
        increaseBag(tweetJson['userId'], retweeted_user_diversity)

    for hashtag in tweetJson['arr_hashtags']:
        increaseBag(hashtag['text'], hashtag_diversity)
    increaseBag(tweetJson['lang'], language_diversity)
    newBag = [w.lower() for w in tweetJson['tweet'].split()]
    if len(newBag)>0:
        for word in newBag:
            increaseBag(word, vocabulary_diversity)
    time = tweetJson['created']
    #Sun Apr 29 11:03:32 +0000 2018
    print("####################################################################")
    print(time)
    ##print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    ##print(time)
    stringTime = time[4:7]+' '+time[8:10]+' '+time[-4:]+' '+time[11:13]+':'+time[14:16]+':'+time[17:19]
    ##print(stringTime)
    datetime_object = datetime.strptime(stringTime, '%b %d %Y %H:%M:%S')
    if datetime_object > timeEnd:
        timeEnd = datetime_object
    if datetime_object < timeStart:
        timeStart = datetime_object

    ##print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + stringTime)
    spreadVelocity = timeEnd - timeStart
    depth_retweets= depth_retweets/numberItem
    ratio_retweets= ratio_retweets/numberItem
    hashtags= hashtags/numberItem
    length= length/numberItem
    exclamations= exclamations/numberItem
    questions= questions/numberItem
    links= links/numberItem
    ##print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    topicRepetition= topicRepetition/numberItem
    replies= replies/numberItem
    spreadVelocity= spreadVelocity.total_seconds()/numberItem
    user_diversity1= calShannon(user_diversity)
    retweeted_user_diversity1= calShannon(retweeted_user_diversity)
    hashtag_diversity1= calShannon(hashtag_diversity)
    language_diversity1= calShannon(language_diversity)
    vocabulary_diversity1= calShannon(vocabulary_diversity)
    ##print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    saveDataToFile()
    return [str(depth_retweets),str(ratio_retweets),str(hashtags),
            str(length),str(exclamations),str(questions),
            str(links),str(topicRepetition),str(replies),
            str(spreadVelocity),str(user_diversity1),
            str(retweeted_user_diversity1),str(hashtag_diversity1),
            str(language_diversity1),str(vocabulary_diversity1)]
    #return [str(), str(), ]

def exportModel(model,filename):
    pickle.dump(model,open(filename,'wb'))

def importModel(filename):
    return pickle.load(open(filename,'rb'))

from sklearn.linear_model import SGDClassifier
#predict data with SGD
def classificationWithSGD(predictData):
    clf = importModel("SGD")
    clf.predict(predictData)

def trainNewData(xTrain, testData ):
    SGDClassifier.partial_fit(xTrain,testData)
    exportModel(xTrain,"SGD")
    return None


if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")

	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)
    sqlContext = sql.SQLContext(sc)
	#Create Kafka Stream to Consume Data Comes From Twitter Topic
	#localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})
    #Parse Twitter Data as json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    #parsed = kafkaStream.map(lambda x: x[1])
    #kafkaStream.saveAsTextFiles('test.txt')
	#Count the number of tweets per Usere
    #lines = parsed.map(lambda x: x[1])
    tweets = parsed.map(getFeature)
    # ##print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    #print('asdljaslkdjaslkjdasjdlkajdlasjkd',len(tweets))
    tweets.pprint()
    #tweets.saveDataToFile("1")
    # vector = np.array(tweets)
    #rdd = tweets.foreachRDD(getRDD)
    #turnIntoVector(rdd)
    # ##print(vector)

	#Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()

#! /usr/bin/env python

import ast
import base64
import json
import os
import re

from datetime import datetime
from operator import itemgetter

import boto3 as aws_api
import geohash
import geojson
import preprocessor as twitter_preprocessor

from persistence.mongodb import MongoDBPersistentClient
from twitter.predictors import SapBasedAgePrediction, SapBasedGenderPrediction


enable_language_detection = ast.literal_eval(os.environ.get('ENABLE_LANGUAGE_DETECTION', False))
enable_language_translation = ast.literal_eval(os.environ.get('ENABLE_LANGUAGE_TRANSLATION', False))
enable_sentiment_analysis = ast.literal_eval(os.environ.get('ENABLE_SENTIMENT_ANALYSIS', False))

age_predictor = SapBasedAgePrediction(lexica_file='./etc/age_lex.csv')
gender_predictor = SapBasedGenderPrediction(lexica_file='./etc/gender_lex.csv')
aws_current_region = os.environ.get('AWS_REGION')
mongo_db_host = os.environ.get('MONGO_DB_HOST')
mongo_db_port = os.environ.get('MONGO_DB_PORT')
mongo_db_username = os.environ.get('MONGO_DB_USERNAME')
mongo_db_password = os.environ.get('MONGO_DB_PASSWORD')
mongo_db_name = os.environ.get('MONGO_DB__NAME')
mongo_db_twitter_collection_name = os.environ.get('MONGO_DB_TWITTER_COLLECTION_NAME')
mongo_db_client = MongoDBPersistentClient(
    mongo_db_host,
    mongo_db_port,
    mongo_db_username,
    mongo_db_password,
    mongo_db_name,
    client_name='lambda-twitter-pymongo'
)


def lambda_handler(event, context):
    preprocessed_tweets = []
    aws_comprehend = aws_api.client(service_name='comprehend', region_name=aws_current_region)
    aws_translate = aws_api.client(service_name='translate', region_name=aws_current_region)

    for record in event.get('records'):
        decoded_data = base64.b64decode(record['data']).decode('utf-8').strip()

        tweet = json.loads(decoded_data)

        # Preprocessing
        # Extrapolate useful data
        twitter_user = tweet.get('user')
        tweet_message = tweet.get('text')
        tweet_source = re.search(r'a href=[\"\'].+[\"\'] rel=[\"\'].+[\"\']\>(.*)\<\/a\>', tweet.get('source', '')).group(1)
        tweet_timestamp = datetime.strptime(tweet.get('created_at'), '%a %b %d %H:%M:%S %z %Y')
        tweet_location_by_device_gps = tweet.get('coordinates', {}) # Device GPS-assigned place of tweet
        tweet_location_by_twitter_place = tweet.get('place', {}) # Twitter user assigned place of tweet
        parsed_tweet_message = twitter_preprocessor.parse(tweet_message)
        tweet_urls = [value.match for value in parsed_tweet_message.urls or []]
        tweet_mentions = [value.match for value in parsed_tweet_message.mentions or []]
        tweet_hashtags = [value.match for value in parsed_tweet_message.hashtags or []]
        tweet_reserved_words = [value.match for value in parsed_tweet_message.reserved_words or []]
        tweet_emojis = [value.match for value in parsed_tweet_message.emojis or []]
        tweet_smileys = [value.match for value in parsed_tweet_message.smileys or []]
        tweet_numbers = [value.match for value in parsed_tweet_message.numbers or []]

        # Geohashing device-defined location
        # TODO: Add administrative division enrichment
        tweet_location_by_device_gps_geohash = ''
        if tweet_location_by_device_gps:
            longitude, latitude = geojson.loads(tweet_location_by_device_gps).get('coordinates')
            tweet_location_by_device_gps_geohash = geohash.encode(latitude, longitude, precision=6)

        # Language Detection based on the Tweet
        tweet_detected_dominant_language = ''
        if enable_language_detection:
            languages = aws_comprehend.detect_dominant_language(Text=tweet_message).get('Languages')
            tweet_detected_languages = sorted(languages, key=itemgetter('Score'), reverse=True)
            tweet_detected_dominant_language = tweet_detected_languages[0].get('LanguageCode')

        # Translate message to english if otherwise
        # NOTE: Predictors are base on english language. If unable to translate, it will try
        # to predict gender and age on the native language of the tweet
        tweet_translated_message = ''
        if enable_language_translation:
            try:
                if tweet_detected_dominant_language != 'en':
                    translation = aws_translate.translate_text(Text=tweet_message, SourceLanguageCode=tweet_detected_dominant_language, TargetLanguageCode='en')
                    tweet_translated_message = translation.get('TranslatedText')
            except aws_translate.exceptions.UnsupportedLanguagePairException:
                tweet_translated_message = ''
                
        text = tweet_translated_message if tweet_translated_message else tweet_message

        # Age Prediction based on the Tweet
        twitter_user_predicted_age_based_on_message = abs(int(age_predictor.predict_age(text)))

        # Gender Prediction based on the Tweet
        twitter_user_predicted_gender_based_on_message = gender_predictor.predict_gender(text)
            
        # Sentiment Detection based on the Tweet
        tweet_based_dominant_sentiment = ''
        tweet_predicted_sentiment_based_on_message = ''
        if enable_sentiment_analysis:
            sentiments = aws_comprehend.detect_sentiment(Text=text, LanguageCode='en')
            tweet_predicted_sentiment_based_on_message = sentiments.get('Sentiment').lower()

            # Retrieve dominant sentiment if detected sentiment is Neutral 
            measured_positive_sentiment = sentiments.get('SentimentScore').get('Positive')
            measured_negative_sentiment = sentiments.get('SentimentScore').get('Negative')
            measured_dominant_sentiment = measured_positive_sentiment - measured_negative_sentiment

            tweet_based_dominant_sentiment = 'neutral'
            if measured_dominant_sentiment > 0:
                tweet_based_dominant_sentiment = 'positive'
            elif measured_dominant_sentiment < 0:
                tweet_based_dominant_sentiment = 'negative'

        # Transformed tweet object
        data_record = {
            'twitterUser': twitter_user,
            'twitterUserPredictedAgeBasedOnMessage': twitter_user_predicted_age_based_on_message,
            'twitterUserPredictedGenderBasedOnMessage': twitter_user_predicted_gender_based_on_message,
            'tweetUserAssignedLocation': tweet_location_by_twitter_place,
            'tweetDeviceAssignedLocation': tweet_location_by_device_gps,
            'tweetDeviceAssignedLocationGeoHash': tweet_location_by_device_gps_geohash,
            'tweetSource': tweet_source,
            'tweetExtrapolatedMessage': tweet_message,
            'tweetExtrapolatedUrls': tweet_urls,
            'tweetExtrapolatedMentions': tweet_mentions,
            'tweetExtrapolatedHashtags': tweet_hashtags,
            'tweetExtrapolatedReservedWords': tweet_reserved_words,
            'tweetExtrapolatedEmojis': tweet_emojis,
            'tweetExtrapolatedSmileys': tweet_smileys,
            'tweetExtrapolatedNumbers': tweet_numbers,
            'tweetDetectedDominantLanguage': tweet_detected_dominant_language,
            'tweetEnglishTranslatedMessage': tweet_translated_message,
            'tweetPredictedSentimentBasedOnMessage': tweet_predicted_sentiment_based_on_message,
            'tweetBasedDominantSentiment': tweet_based_dominant_sentiment,
            'tweetTimestamp': tweet_timestamp,
        }

        preprocessed_tweet = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(data_record).encode('utf-8')).decode('utf-8')
        }
        
        preprocessed_tweets.append(preprocessed_tweet)

    # Persist to MongoDB
    mongo_db_client.insert_many(mongo_db_twitter_collection_name, preprocessed_tweets)

    # Archive to AWS S3
    return {'records': preprocessed_tweets}

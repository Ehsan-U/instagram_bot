""" Requests based """

import argparse
import json
import re
import requests
from constant import HEADERS
from copy import deepcopy
from datetime import datetime
from urlextract import URLExtract
from pprint import pprint
from urllib.parse import urlparse
import pytz
import random
import time



class InstaScraper():
    link_extractor = URLExtract()
    email_regx = re.compile('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}', re.IGNORECASE)
    sleep_range = (5.0, 10.0)


    def request(self, method, url, **kwargs):
        try:
            self.go_sleep(self.sleep_range)
            response = requests.request(method, url, **kwargs)
        except Exception:
            return None
        else:
            return response


    def load_session(self):
        with open("session.json", 'r') as f:
            config = json.load(f)
            app_id = config.get('app_id')
            self.session_cookies = config.get("cookies")
            self.session_headers = self.update_headers(self.session_cookies, deepcopy(HEADERS), app_id)


    def parse_searchResults(self, response, cb_kwargs):
        data = json.loads(response.content)
        for user in data.get("users"):
            try:
                username = user.get('user').get('username')
                url = f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}'
                headers = self.update_referer(self.session_headers, username)
                response = self.request('GET', url, headers=headers, cookies=self.session_cookies)
                self.parse_user(response, **cb_kwargs)
            except Exception as e:
                print(" [+] error in parse_searchResults")


    def parse_user(self, response, keyword, iDOutRequest, minimumNumberofSubscribers, lastUploadCutoffDate):
        user = json.loads(response.content).get("data",{}).get("user")
        if user:
            userId = user.get("id")
            profileName = user.get("full_name")
            profileUrl = f"https://www.instagram.com/{user.get('username')}/"
            channelName = next(i for i in urlparse(profileUrl).path.split('/') if i != '')
            profileDescription = user.get("biography",'') + ' ' + user.get("external_url") if user.get("external_url") else ''
            posts = user.get("edge_owner_to_timeline_media",{}).get("edges")
            if posts:
                posts = posts[:30]
                metric_LastUploadDate = self.to_date(posts[0].get("node",{}).get("taken_at_timestamp"))
                subscribers = user.get("edge_followed_by",{}).get('count',0)
                keyword = keyword
                emailfromChannelDescription = ",".join(self.email_regx.findall(profileDescription))
                canMessage = user.get("business_category_name")
                allow = self.allowed(subscribers, minimumNumberofSubscribers, lastUploadCutoffDate, metric_LastUploadDate)
                item = {
                    "keyword":keyword,
                    "iDOutRequest": iDOutRequest,
                    "channelId":userId,
                    "channelName":channelName,
                    "personName": profileName,
                    "channelURL":profileUrl,
                    "metric_Subscribers":subscribers,
                    "channelDescription":profileDescription,
                    "canMessage":True if canMessage else False,
                    "metric_LastUploadDate":self.to_us_eastern(metric_LastUploadDate),
                    "emailfromChannelDescription":emailfromChannelDescription,
                }
                if allow:
                    pprint(item)


    def allowed(self, metric_Subscribers, min_subs, cutoffdays, metric_LastUploadDate):
        if metric_Subscribers <= min_subs:
            return False
        else:
            today_str = datetime.now().strftime("%Y-%m-%d")
            age = self.days_between(metric_LastUploadDate, today_str)
            if age >= cutoffdays:
                return False
        return True
    

    @staticmethod
    def update_headers(cookies, headers, app_id):
        headers['X-Ig-App-Id'] = app_id
        for key, val in cookies.items():
            if 'csrftoken' in key.lower():
                headers['X-Csrftoken'] = val
        return headers
    

    @staticmethod
    def update_referer(headers, username):
        headers['Referer'] = f'https://www.instagram.com/{username}/'
        return headers


    @staticmethod
    def to_date(timestamp):
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


    @staticmethod
    def days_between(date1, date2):
        date1_obj = datetime.strptime(date1, '%Y-%m-%d')
        date2_obj = datetime.strptime(date2, '%Y-%m-%d')
        delta = date2_obj - date1_obj
        return delta.days


    @staticmethod
    def get_text(posts):
        text = ''
        for post in posts:
            edges = post.get("node",{}).get("edge_media_to_caption",{}).get("edges",{})
            if edges:
                text += edges[0].get("node",{}).get("text")
        return text


    @staticmethod
    def to_us_eastern(date_str):
        given_date = datetime.strptime(date_str, "%Y-%m-%d")
        us_eastern = pytz.timezone('US/Eastern')
        localized_date = us_eastern.localize(given_date)
        formatted_date = localized_date.strftime('%Y-%m-%d %H:%M:%S %Z')
        return formatted_date
    

    @staticmethod
    def go_sleep(sleep_range):
        sleep_time = random.uniform(*sleep_range)
        time.sleep(sleep_time)


    @classmethod
    def globalize(cls, keyword):
        for key, value in keyword.items():
            setattr(cls, key, value)


    def crawl(self, keywords):
        self.load_session()
        if self.session_cookies:
            for keyword in keywords:
                self.globalize(keyword)
                url = f'https://www.instagram.com/api/v1/web/search/topsearch/?context=blended&query={self.keyword}&search_surface=web_top_search'
                response = self.request('GET', url, headers=self.session_headers, cookies=self.session_cookies)
                if response:
                    self.parse_searchResults(response, cb_kwargs=keyword)



crawler = InstaScraper()
crawler.crawl(keywords=[{"keyword":'scrapy',"iDOutRequest":1, "minimumNumberofSubscribers":10, "lastUploadCutoffDate":30}])
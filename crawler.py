import argparse
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import re
from constant import HEADERS
from scrapy import signals
from copy import deepcopy
from datetime import datetime
from urlextract import URLExtract
from pprint import pprint
from urllib.parse import urlparse
import pytz



class InstaScraper(scrapy.Spider):
    name = 'insta_spider'
    link_extractor = URLExtract()
    email_regx = re.compile('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}', re.IGNORECASE)


    def start_requests(self):
        for keyword in self.keywords:
            url = f'https://www.instagram.com/api/v1/web/search/topsearch/?context=blended&query={keyword.get("keyword")}&search_surface=web_top_search'
            headers = self.session_headers
            yield scrapy.Request(url, callback=self.parse_searchResults, headers=headers, cookies=self.session_cookies, cb_kwargs={"keyword":keyword.get("keyword"), "iDOutRequest":keyword.get("iDOutRequest"), "min_subs":keyword.get("minimumNumberofSubscribers"),"cutoffdays":keyword.get("lastUploadCutoffDate")})


    def parse_searchResults(self, response, keyword, iDOutRequest, min_subs, cutoffdays):
        data = json.loads(response.body)
        for user in data.get("users"):
            username = user.get('user').get('username')
            url = f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}'
            headers = self.update_referer(self.session_headers, username)
            yield scrapy.Request(url, callback=self.parse_user, headers=headers, cookies=self.session_cookies, cb_kwargs={"keyword":keyword, "iDOutRequest":iDOutRequest, "min_subs":min_subs,"cutoffdays":cutoffdays})


    def parse_user(self, response, keyword, iDOutRequest, min_subs, cutoffdays):
        user = json.loads(response.body).get("data",{}).get("user")
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
                allow = self.allowed(subscribers, min_subs, cutoffdays, metric_LastUploadDate)
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


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(InstaScraper, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider
    

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


    def spider_opened(self, spider):
        with open("session.json", 'r') as f:
            config = json.load(f)
            app_id = config.get('app_id')
            self.session_cookies = config.get("cookies")
            self.session_headers = self.update_headers(self.session_cookies, deepcopy(HEADERS), app_id)
            
    
########################



crawler = CrawlerProcess(settings={
    "HTTPCACHE_ENABLED": True,
    "DOWNLOAD_DELAY": 10,
    "CONCURRENT_REQUESTS": 1,
})
crawler.crawl(InstaScraper, keywords=[{"keyword":'scrapy',"iDOutRequest":1, "minimumNumberofSubscribers":10, "lastUploadCutoffDate":30}])
crawler.start()
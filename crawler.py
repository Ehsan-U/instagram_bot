import scrapy
from scrapy.crawler import CrawlerProcess
import json
import re
from constant import HEADERS
from scrapy import signals
from copy import deepcopy
from datetime import datetime
from urlextract import URLExtract




class InstaScraper(scrapy.Spider):
    name = 'insta_spider'
    link_extractor = URLExtract()
    email_regx = re.compile('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}', re.IGNORECASE)


    def start_requests(self):
        for keyword in self.keywords:
            url = f'https://www.instagram.com/api/v1/web/search/topsearch/?context=blended&query={keyword.get("keyword")}&search_surface=web_top_search'
            headers = self.session_headers
            yield scrapy.Request(url, callback=self.parse_searchResults, headers=headers, cookies=self.session_cookies, cb_kwargs={"keyword":keyword.get("keyword"), "min_subs":keyword.get("minimumNumberofSubscribers"),"cutoffdays":keyword.get("lastUploadCutoffDate")})


    def parse_searchResults(self, response, keyword, min_subs, cutoffdays):
        data = json.loads(response.body)
        for user in data.get("users"):
            username = user.get('user').get('username')
            url = f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}'
            headers = self.update_referer(self.session_headers, username)
            yield scrapy.Request(url, callback=self.parse_user, headers=headers, cookies=self.session_cookies, cb_kwargs={"keyword":keyword, "min_subs":min_subs,"cutoffdays":cutoffdays})
            # break


    def parse_user(self, response, keyword, min_subs, cutoffdays):
        user = json.loads(response.body).get("data",{}).get("user")
        if user:
            userId = user.get("id")
            profileName = user.get("full_name")
            profileUrl = f"https://www.instagram.com/{user.get('username')}/"
            profileDescription = user.get("biography")
            posts = user.get("edge_owner_to_timeline_media",{}).get("edges")
            if posts:
                posts = posts[:30]
                metric_LastUploadDate = self.to_date(posts[0].get("node",{}).get("taken_at_timestamp"))
                metric_Last30PostsDatePostedAndComments = ",".join([f'{self.to_date(post.get("node", {}).get("taken_at_timestamp"))}:{post.get("node",{}).get("edge_media_to_comment",{}).get("count")}' for post in posts])
                metric_Last30PostsDatePostedAndReactions = ",".join([f'{self.to_date(post.get("node", {}).get("taken_at_timestamp"))}:{post.get("node",{}).get("edge_liked_by",{}).get("count")}' for post in posts])
                linksFrom30LatestPosts = ",".join(self.link_extractor.find_urls(self.get_text(posts)))
                text_30LatestPostsDescription = self.get_text(posts)
                emailFromLatestPostDescription = ",".join(self.email_regx.findall(self.get_text(posts[0:1])))
            metricProfileNumberOfPosts = user.get("edge_owner_to_timeline_media",{}).get("count",0)
            subscribers = user.get("edge_followed_by",{}).get('count',0)
            linksFromProfileDescription = ",".join(self.link_extractor.find_urls(profileDescription))
            keyword = keyword
            emailfromChannelDescription = ",".join(self.email_regx.findall(profileDescription))
            allow = self.allowed(subscribers, min_subs, cutoffdays, metric_LastUploadDate)
            item = {
                "channelId":userId,
                "channelName":profileName,
                "channelURL":profileUrl,
                "metric_Subscribers":subscribers,
                "channelDescription":profileDescription,
                "canMessage":True,
                "metric_ChannelNumberOfPosts":metricProfileNumberOfPosts,
                "metric_LastUploadDate":metric_LastUploadDate,
                "metric_Last30PostsDatePosted&NumberofComments":metric_Last30PostsDatePostedAndComments,
                "metric_Last30PostsDatePosted&Reactions":metric_Last30PostsDatePostedAndReactions,
                "text_30LatestPostsDescription":text_30LatestPostsDescription,
                "linksfrom30LatestPosts":linksFrom30LatestPosts,
                "emailFromLatestPostDescription":emailFromLatestPostDescription,
                "linksFromProfileDescription":linksFromProfileDescription,
                "emailfromChannelDescription":emailfromChannelDescription,
            }
            if allow:
                yield item


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


    def spider_opened(self, spider):
        with open("session.json", 'r') as f:
            config = json.load(f)
            app_id = config.get('app_id')
            self.session_cookies = config.get("cookies")
            self.session_headers = self.update_headers(self.session_cookies, deepcopy(HEADERS), app_id)
            
    

crawler = CrawlerProcess(settings={
    "HTTPCACHE_ENABLED": True,
    "DOWNLOAD_DELAY": 10,
    "CONCURRENT_REQUESTS": 1,
})
crawler.crawl(InstaScraper, keywords=[{"keyword":'scrapy',"iDOutRequest":1, "minimumNumberofSubscribers":10, "lastUploadCutoffDate":100}])
crawler.start()
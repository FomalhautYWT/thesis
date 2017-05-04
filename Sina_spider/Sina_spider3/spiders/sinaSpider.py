# encoding=utf-8
# ------------------------------------------
#   版本：3.0
#   日期：2016-12-01
#   作者：九茶<https://blog.csdn.net/bone_ace>
# ------------------------------------------

import sys
import logging
import datetime
import requests
import re
from lxml import etree
from Sina_spider3.weiboID import weiboID
from Sina_spider3.weiboKeyword import Keywords
from Sina_spider3.scrapy_redis.spiders import RedisSpider
from scrapy.selector import Selector
from scrapy.http import Request
from Sina_spider3.items import TweetsItem, InformationItem, RelationshipsItem,KeywordsItem,CommentsItem

reload(sys)
sys.setdefaultencoding('utf8')

dateDelta = 30
class Spider(RedisSpider):
    name = "SinaSpider"
    host = "https://weibo.cn"
    redis_key = "SinaSpider:start_urls"
    keywords = list(set(Keywords))
    start_urls = list(set(weiboID))
    logging.getLogger("requests").setLevel(logging.WARNING)  # 将requests的日志级别设成WARNING

    def start_requests(self):
        
        for keyword in self.keywords:

            for i in range(dateDelta):
                now = datetime.datetime.now()
                deltaDay = datetime.timedelta(days=i)
                starttime = now - deltaDay
                starttime = starttime.strftime('%Y%m%d')
                print starttime
                url_tweets_search = "https://weibo.cn/search/mblog?&keyword=%s&filter=hasori&sort=hot&page=1&starttime=%s&endtime=%s" % (keyword , starttime , starttime)
                yield Request(url=url_tweets_search, meta={"keyword":keyword}, callback=self.parse_keyword)

        #for uid in self.start_urls:
        #    yield Request(url="https://weibo.cn/%s/info" % uid, callback=self.parse_information)

    def parse_information(self, response):
        """ 抓取个人信息 """
        informationItem = InformationItem()
        selector = Selector(response)
        ID = re.findall('(\d+)/info', response.url)[0]
        try:
            text1 = ";".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text()
            nickname = re.findall('昵称[：:]?(.*?);'.decode('utf8'), text1)
            gender = re.findall('性别[：:]?(.*?);'.decode('utf8'), text1)
            place = re.findall('地区[：:]?(.*?);'.decode('utf8'), text1)
            briefIntroduction = re.findall('简介[：:]?(.*?);'.decode('utf8'), text1)
            birthday = re.findall('生日[：:]?(.*?);'.decode('utf8'), text1)
            sexOrientation = re.findall('性取向[：:]?(.*?);'.decode('utf8'), text1)
            sentiment = re.findall('感情状况[：:]?(.*?);'.decode('utf8'), text1)
            vipLevel = re.findall('会员等级[：:]?(.*?);'.decode('utf8'), text1)
            authentication = re.findall('认证[：:]?(.*?);'.decode('utf8'), text1)
            url = re.findall('互联网[：:]?(.*?);'.decode('utf8'), text1)

            informationItem["_id"] = ID
            if nickname and nickname[0]:
                informationItem["NickName"] = nickname[0].replace(u"\xa0", "")
            if gender and gender[0]:
                informationItem["Gender"] = gender[0].replace(u"\xa0", "")
            if place and place[0]:
                place = place[0].replace(u"\xa0", "").split(" ")
                informationItem["Province"] = place[0]
                if len(place) > 1:
                    informationItem["City"] = place[1]
            if briefIntroduction and briefIntroduction[0]:
                informationItem["BriefIntroduction"] = briefIntroduction[0].replace(u"\xa0", "")
            if birthday and birthday[0]:
                try:
                    birthday = datetime.datetime.strptime(birthday[0], "%Y-%m-%d")
                    informationItem["Birthday"] = birthday - datetime.timedelta(hours=8)
                except Exception:
                    informationItem['Birthday'] = birthday[0]   # 有可能是星座，而非时间
            if sexOrientation and sexOrientation[0]:
                if sexOrientation[0].replace(u"\xa0", "") == gender[0]:
                    informationItem["SexOrientation"] = "同性恋"
                else:
                    informationItem["SexOrientation"] = "异性恋"
            if sentiment and sentiment[0]:
                informationItem["Sentiment"] = sentiment[0].replace(u"\xa0", "")
            if vipLevel and vipLevel[0]:
                informationItem["VIPlevel"] = vipLevel[0].replace(u"\xa0", "")
            if authentication and authentication[0]:
                informationItem["Authentication"] = authentication[0].replace(u"\xa0", "")
            if url:
                informationItem["URL"] = url[0]

            try:
                urlothers = "https://weibo.cn/attgroup/opening?uid=%s" % ID
                r = requests.get(urlothers, cookies=response.request.cookies, timeout=5)
                if r.status_code == 200:
                    selector = etree.HTML(r.content)
                    texts = ";".join(selector.xpath('//body//div[@class="tip2"]/a//text()'))
                    if texts:
                        num_tweets = re.findall('微博\[(\d+)\]'.decode('utf8'), texts)
                        num_follows = re.findall('关注\[(\d+)\]'.decode('utf8'), texts)
                        num_fans = re.findall('粉丝\[(\d+)\]'.decode('utf8'), texts)
                        if num_tweets:
                            informationItem["Num_Tweets"] = int(num_tweets[0])
                        if num_follows:
                            informationItem["Num_Follows"] = int(num_follows[0])
                        if num_fans:
                            informationItem["Num_Fans"] = int(num_fans[0])
            except Exception, e:
                pass
        except Exception, e:
            pass
        else:
            yield informationItem
        yield Request(url="https://weibo.cn/%s/profile?filter=1&page=1" % ID, callback=self.parse_tweets, dont_filter=True)
        yield Request(url="https://weibo.cn/%s/follow" % ID, callback=self.parse_relationship, dont_filter=True)
        yield Request(url="https://weibo.cn/%s/fans" % ID, callback=self.parse_relationship, dont_filter=True)

    def parse_tweets(self, response):
        """ 抓取微博数据 """
        selector = Selector(response)
        ID = re.findall('(\d+)/profile', response.url)[0]
        divs = selector.xpath('body/div[@class="c" and @id]')
        for div in divs:
            try:
                tweetsItems = TweetsItem()
                id = div.xpath('@id').extract_first()  # 微博ID
                content = div.xpath('div/span[@class="ctt"]//text()').extract()  # 微博内容
                cooridinates = div.xpath('div/a/@href').extract()  # 定位坐标
                like = re.findall('赞\[(\d+)\]'.decode('utf8'), div.extract())  # 点赞数
                transfer = re.findall('转发\[(\d+)\]'.decode('utf8'), div.extract())  # 转载数
                comment = re.findall('评论\[(\d+)\]'.decode('utf8'), div.extract())  # 评论数
                others = div.xpath('div/span[@class="ct"]/text()').extract()  # 求时间和使用工具（手机或平台）

                tweetsItems["_id"] = ID + "-" + id
                tweetsItems["ID"] = ID
                if content:
                    tweetsItems["Content"] = " ".join(content).strip('[位置]'.decode('utf8'))  # 去掉最后的"[位置]"
                if cooridinates:
                    cooridinates = re.findall('center=([\d.,]+)', cooridinates[0])
                    if cooridinates:
                        tweetsItems["Co_oridinates"] = cooridinates[0]
                if like:
                    tweetsItems["Like"] = int(like[0])
                if transfer:
                    tweetsItems["Transfer"] = int(transfer[0])
                if comment:
                    tweetsItems["Comment"] = int(comment[0])
                if others:
                    others = others[0].split('来自'.decode('utf8'))
                    tweetsItems["PubTime"] = others[0].replace(u"\xa0", "")
                    if len(others) == 2:
                        tweetsItems["Tools"] = others[1].replace(u"\xa0", "")
                yield tweetsItems
            except Exception, e:
                pass

        url_next = selector.xpath('body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="下页"]/@href'.decode('utf8')).extract()
        if url_next:
            yield Request(url=self.host + url_next[0], callback=self.parse_tweets, dont_filter=True,priority=1)

    def parse_relationship(self, response):
        """ 打开url爬取里面的个人ID """
        selector = Selector(response)
        if "/follow" in response.url:
            ID = re.findall('(\d+)/follow', response.url)[0]
            flag = True
        else:
            ID = re.findall('(\d+)/fans', response.url)[0]
            flag = False
        urls = selector.xpath('//a[text()="关注他" or text()="关注她"]/@href'.decode('utf')).extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        for uid in uids:
            relationshipsItem = RelationshipsItem()
            relationshipsItem["Host1"] = ID if flag else uid
            relationshipsItem["Host2"] = uid if flag else ID
            yield relationshipsItem
            yield Request(url="https://weibo.cn/%s/info" % uid, callback=self.parse_information)

        next_url = selector.xpath('//a[text()="下页"]/@href'.decode('utf8')).extract()
        if next_url:
            yield Request(url=self.host + next_url[0], callback=self.parse_relationship, dont_filter=True)
    def parse_comment(self, response):
        TWEET_URL = response.meta["TWEET_URL"] #微博URL
        selector = Selector(response)
        OriginContents = selector.xpath('body/div[@class="c" and @id][position()=1]/div[1]/span[@class="ctt"]/text()').extract()
        comments = selector.xpath('body/div[@class="c" and @id][position()>1]')
        for comment in comments:
            commentsItems = CommentsItem()
            _id = comment.xpath('@id').extract_first()  # 微博ID
            ID = comment.xpath('a[1]/@href').extract_first()  # 评论用户ID
            Content = comment.xpath('span[@class="ctt"]/text()').extract()  # 评论内容
            Like = re.findall(u'\u8d5e\[(\d+)\]', comment.extract())  # 点赞数
            NickName = comment.xpath('a[1]/text()').extract_first()  # 评论用户昵称
            others = comment.xpath('span[@class="ct"]/text()').extract_first() # 求时间和使用工具（手机或平台）

            commentsItems["_id"] = _id
            commentsItems["ID"] = ID
            commentsItems["TWEET_URL"] = TWEET_URL
            OriginContent = ""
            for oc in OriginContents:
                OriginContent += oc # 原始微博内容
            commentsItems["OriginContent"] = OriginContent  # 原始微博内容
            if Content:
                commentsItems["Content"] = Content[-1]
            if Like:
                commentsItems["Like"] = int(Like[0])
            if NickName:
                commentsItems["NickName"] = NickName
            if others:

                others = others.split(u"\u6765\u81ea")  # 按"来自"分割

                commentsItems["PubTime"] = others[0]
                if len(others) == 2:
                    commentsItems["Tools"] = others[1]
            yield commentsItems

            url_next = selector.xpath(
                u'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
            if url_next:
                yield Request(url=self.host + url_next[0], meta={"TWEET_URL": TWEET_URL}, callback=self.parse_comment)

    def parse_repost(self, response):
        """ 按url爬取某条转发 """
        TWEET_URL = response.meta["TWEET_URL"]  # 微博URL
        selector = Selector(response)
        OriginContents = selector.xpath(
            'body/div[@class="c" and @id][position()=1]/div[1]/span[@class="ctt"]/text()').extract()
        reposts = selector.xpath('body/div[@class="c"][position()>2]')
        for repost in reposts:
            repostsItems = RepostsItem()
            ID = repost.xpath('a[1]/@href').extract_first()  # 转发用户ID
            Content = re.compile(u'<.*?[^>]*?>.*?</.*?>|</div>').sub('', repost.extract())  # 转发回复内容    使用正则表达式删除所有带标签内容  vip用户评论有bug待修改
            Like = re.findall(u'\u8d5e\[(\d+)\]', repost.extract())  # 点赞数
            NickName = repost.xpath('a[1]/text()').extract_first()  # 转发用户昵称
            others = repost.xpath('span[@class="ct"]/text()').extract_first()  # 求时间和使用工具（手机或平台）

            repostsItems["ID"] = ID
            repostsItems["TWEET_URL"] = TWEET_URL
            OriginContent = ""
            for oc in OriginContents:
                OriginContent += oc  # 原始微博内容
            repostsItems["OriginContent"] = OriginContent  # 原始微博内容
            if Content:
                repostsItems["Content"] = Content
            if Like:
                repostsItems["Like"] = int(Like[0])
            if NickName:
                repostsItems["NickName"] = NickName
            if others:

                others = others.split(u"\u6765\u81ea")  # 按"来自"分割
                pubtime = self.parse_date(others[0])
                repostsItems["PubTime"] = pubtime
                if len(others) == 2:
                    repostsItems["Tools"] = others[1]
            yield repostsItems

            url_next = selector.xpath(
                u'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
            if url_next:
                yield Request(url=self.host + url_next[0], meta={"TWEET_URL": TWEET_URL}, callback=self.parse_repost)

    def parse_keyword(self, response):
        """ 按关键词抓取微博数据 """
        selector = Selector(response)
        tweets = selector.xpath('body/div[@class="c" and @id]')
        for tweet in tweets:
            keywordsItems = KeywordsItem()
            id = tweet.xpath('@id').extract_first()  # 微博ID
            userURL = tweet.xpath('div[1]/a[1]/@href').extract_first() # 用户URL
            nickname = tweet.xpath('div[1]/a[1]/text()').extract_first() # 用户昵称
            contents = tweet.xpath('div/span[@class="ctt"]').xpath('string(.)').extract()  # 微博内容
            cooridinates = tweet.xpath('div[1]/a[2]/@href').extract_first()  # 定位坐标
            like = re.findall(u'\u8d5e\[(\d+)\]', tweet.extract())  # 点赞数
            transfer = re.findall(u'\u8f6c\u53d1\[(\d+)\]', tweet.extract())  # 转载数
            comment = re.findall(u'\u8bc4\u8bba\[(\d+)\]', tweet.extract())  # 评论数
            others = tweet.xpath('div/span[@class="ct"]/text()').extract_first()  # 求时间和使用工具（手机或平台）

            keywordsItems["Keyword"] = response.meta["keyword"]
            keywordsItems["UserURL"] = userURL
            keywordsItems["NickName"] = nickname
            keywordsItems["_id"] = id
            if contents:
                content = ""
                for c in contents:
                    content += c  # 原始微博内容
                if (content.endswith(u"[\u4f4d\u7f6e]")):
                    keywordsItems["Content"] = content[1:].strip(u"[\u4f4d\u7f6e]")  # 去掉最后的"[位置]"
                else:
                    keywordsItems["Content"] = content[1:]
            if cooridinates:
                cooridinates = re.findall('center=([\d|.|,]+)', cooridinates)
                if cooridinates:
                    keywordsItems["Co_oridinates"] = cooridinates[0]
            if like:
                keywordsItems["Like"] = int(like[0])
            if transfer:
                keywordsItems["Transfer"] = int(transfer[0])
            if comment:
                keywordsItems["Comment"] = int(comment[0])
            if others:
                others = others.split(u'\u6765\u81ea')
                pubtime = self.parse_date(others[0])
                keywordsItems["PubTime"] = pubtime
                if len(others) == 2:
                    keywordsItems["Tools"] = others[1]
            yield keywordsItems
        url_next = selector.xpath(
            u'body/div[@class="pa" and @id="pagelist"]/form/div/a[text()="\u4e0b\u9875"]/@href').extract()
        if url_next:
            yield Request(url=self.host + url_next[0], meta={"keyword": response.meta["keyword"]}, callback=self.parse_keyword)
    def parse_date(self, t):
        ''' 转换日期格式 '''
        nowtime = datetime.datetime
        if re.findall(u'\u6708', t):   # 转换*月*日格式
            dates = re.split(u'[\u6708\u65e5 ]', t)
            return nowtime(nowtime.now().year, int(dates[0]), int(dates[1]), int(dates[3][:2]), int(dates[3][-2:])).isoformat()
        if re.findall(u'\u5206\u949f\u524d', t):  # 转换*分钟前格式
            dates = re.split(u'\u5206\u949f\u524d', t)
            return nowtime.utcfromtimestamp(int(time.time())+8*3600-int(dates[0])*60).isoformat()
        if re.findall(u'\u79d2\u524d', t):  # 转换*秒前格式
            dates = re.split(u'\u79d2\u524d', t)
            return nowtime.utcfromtimestamp(int(time.time()) + 8 * 3600 - int(dates[0])).isoformat()
        if re.findall(u'\u4eca\u5929', t):  # 转换今天***格式
            dates = re.split(u'\u4eca\u5929 ', t)
            return nowtime(nowtime.now().year, nowtime.now().month, nowtime.now().day, int(dates[1][:2]), int(dates[1][-2:])).isoformat()
        if re.findall('.*?-.*?-.*?', t):  # 转换普通格式
            return nowtime.strptime(t[:-1], u"%Y-%m-%d %H:%M:%S").isoformat()
        return t

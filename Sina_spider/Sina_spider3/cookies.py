# encoding=utf-8
# ------------------------------------------
#   版本：3.0
#   日期：2016-12-01
#   作者：九茶<http://blog.csdn.net/bone_ace>
# ------------------------------------------

import os
import time
import json
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import logging
from yumdama import identify

IDENTIFY = 1  # 验证码输入方式:        1:看截图aa.png，手动输入     2:云打码
dcap = dict(DesiredCapabilities.PHANTOMJS)  # PhantomJS需要使用老版手机的user-agent，不然验证码会无法通过
dcap["phantomjs.page.settings.userAgent"] = (
    "Mozilla/5.0 (Linux; U; Android 2.3.6; en-us; Nexus S Build/GRK39F) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
)
logger = logging.getLogger(__name__)
logging.getLogger("selenium").setLevel(logging.WARNING)  # 将selenium的日志级别设成WARNING，太烦人

myWeiBo = [
]


#def getCookie(account, password):
#    """ 获取一个账号的Cookie """
#    try:
#        browser = webdriver.PhantomJS(desired_capabilities=dcap)
#        browser.get("https://weibo.cn/login/")
#        time.sleep(1)
#
#        failure = 0
#        while "微博" in browser.title and failure < 5:
#            failure += 1
#            browser.save_screenshot("aa.png")
#            username = browser.find_element_by_name("mobile")
#            username.clear()
#            username.send_keys(account)
#
#            psd = browser.find_element_by_xpath('//input[@type="password"]')
#            psd.clear()
#            psd.send_keys(password)
#            try:
#                code = browser.find_element_by_name("code")
#                code.clear()
#                if IDENTIFY == 1:
#                    code_txt = raw_input("请查看路径下新生成的aa.png，然后输入验证码:")  # 手动输入验证码
#                else:
#                    from PIL import Image
#                    img = browser.find_element_by_xpath('//form[@method="post"]/div/img[@alt="请打开图片显示"]')
#                    x = img.location["x"]
#                    y = img.location["y"]
#                    im = Image.open("aa.png")
#                    im.crop((x, y, 100 + x, y + 22)).save("ab.png")  # 剪切出验证码
#                    code_txt = identify()  # 验证码打码平台识别
#                code.send_keys(code_txt.decode('utf-8'))
#            except Exception, e:
#                pass
#
#            commit = browser.find_element_by_name("submit")
#            commit.click()
#            time.sleep(3)
#            if "我的首页" not in browser.title:
#                time.sleep(4)
#            if '未激活微博' in browser.page_source:
#                print '账号未开通微博'
#                return {}
#            
#        cookie = {}
#        if "我的首页" in browser.title:
#            for elem in browser.get_cookies():
#                cookie[elem["name"]] = elem["value"]
#            logger.warning("Get Cookie Success!( Account:%s )" % account)
#        return json.dumps(cookie)
#    except Exception, e:
#        logger.warning("Failed %s!" % account)
#        return ""
#    finally:
#        try:
#            browser.quit()
#        except Exception, e:
#            pass

def getCookie(account,password):
	
	browser = webdriver.Chrome()
	browser.set_window_size(1050,840)
	browser.get('https://passport.weibo.cn/signin/login?entry=mweibo&r=http://weibo.cn/')
	time.sleep(1)
	name = browser.find_element_by_id('loginName')
	psw = browser.find_element_by_id('loginPassword')
	login = browser.find_element_by_id('loginAction')
	name.send_keys(account)
	psw.send_keys(password)
	login.click()
	time.sleep(10)
	cookie = {}
	for elem in browser.get_cookies():
		cookie[elem["name"]] = elem["value"]
	browser.close()
	return json.dumps(cookie)


def initCookie(rconn, spiderName):
    """ 获取所有账号的Cookies，存入Redis。如果Redis已有该账号的Cookie，则不再获取。 """
    for weibo in myWeiBo:
        if rconn.get("%s:Cookies:%s--%s" % (spiderName, weibo[0], weibo[1])) is None:  # 'SinaSpider:Cookies:账号--密码'，为None即不存在。
            cookie = getCookie(weibo[0], weibo[1])
            if len(cookie) > 0:
                rconn.set("%s:Cookies:%s--%s" % (spiderName, weibo[0], weibo[1]), cookie)
    cookieNum = "".join(rconn.keys()).count("SinaSpider:Cookies")
    logger.warning("The num of the cookies is %s" % cookieNum)
    if cookieNum == 0:
        logger.warning('Stopping...')
        os.system("pause")


def updateCookie(accountText, rconn, spiderName):
    """ 更新一个账号的Cookie """
    account = accountText.split("--")[0]
    password = accountText.split("--")[1]
    cookie = getCookie(account, password)
    if len(cookie) > 0:
        logger.warning("The cookie of %s has been updated successfully!" % account)
        rconn.set("%s:Cookies:%s" % (spiderName, accountText), cookie)
    else:
        logger.warning("The cookie of %s updated failed! Remove it!" % accountText)
        removeCookie(accountText, rconn, spiderName)


def removeCookie(accountText, rconn, spiderName):
    """ 删除某个账号的Cookie """
    rconn.delete("%s:Cookies:%s" % (spiderName, accountText))
    cookieNum = "".join(rconn.keys()).count("SinaSpider:Cookies")
    logger.warning("The num of the cookies left is %s" % cookieNum)
    if cookieNum == 0:
        logger.warning("Stopping...")
        os.system("pause")
# -*- coding: utf-8 -*-

import asyncio
import time
import re
from asyncio import Queue, QueueEmpty, QueueFull

import aiohttp
from lxml import etree

CLIENT_SESSION_COUNT,PROXY_USED,PROXY_KEY_URL,PROXY_TIMES_SIZE = None,None,None,None
with open('config.xml', 'rb') as fp:
    el_config = etree.XML(fp.read())

    CLIENT_SESSION_COUNT = int(el_config.xpath('//downloader/client_sesseion_count/text()', smart_strings=False)[0])
    PROXY_USED = int(el_config.xpath('//downloader/proxy/used/text()', smart_strings=False)[0])
    if PROXY_USED:
        PROXY_USED = True
        PROXY_KEY_URL = str(el_config.xpath('//downloader/proxy/key/text()', smart_strings=False)[0])
        PROXY_TIMES_SIZE = int(el_config.xpath('//downloader/proxy/times_size/text()', smart_strings=False)[0])
    else:
        PROXY_USED = False
del el_config, fp

DEFAULT_REQUEST_HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}
PROXY_IP_PATTERN = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\:\d{1,5}')

class DownLoadError(Exception):
    """下载时发生任何异常，都统一到抛出此异常"""
    def __init__(self, err):
        self.err = err

class ProxyFectchError(Exception):
    def __init__(self, err):
        self.err = err

class DownloaderDirector():
    def __init__(self):
        """下载管理器维护着一个ClientSession队列，为空闲session队列。
        将download协程构建函数的选择放到手动初始化那步统一判断"""
        self.idle_session_pool = Queue(maxsize=CLIENT_SESSION_COUNT)
        self.proxy_used = PROXY_USED
        self.download = None
        self.proxyDirector = None
        
    async def init_idle_session_pool(self):
        """空闲队列初始化时机：
        因为对象初始化时，返回对象本身，所以__init__不能是携程，也即是不能在__init__内部初始化空闲队列；在创建下载管理器后，马上进行空闲session队列初始化工作。
        session就相当于一个打开的浏览器，一个session用于多次页面访问下载。
        类比为一开始就打开若干个浏览器，全部呈空闲状态，所以全部放入空闲队列。
        
        需要根据是否需要代理ip，为download选择合适的协程构建函数，避免每次下载时判断。

        如果要用到代理ip，还需要创建并初始化一个proxyDirector"""
        while True:
            try:
                session = aiohttp.ClientSession(headers=DEFAULT_REQUEST_HEADERS, timeout=aiohttp.ClientTimeout(60), cookie_jar=aiohttp.DummyCookieJar())
                self.idle_session_pool.put_nowait(session)
            except QueueFull:
                # 最后创建的session，插入不到队列中，需要close
                await session.close()
                break
        
        if self.proxy_used:
            self.download = self.download_proxy
            self.proxyDirector = ProxyDirector()
        else:
            self.download = self.download_straight

    async def download_proxy(self, req, headers={}):
        """通过代理ip下载，比直接下载多一个获取代理ip的步骤"""
        try:
            # 获取代理ip
            proxy = await self.proxyDirector.get_proxy_ip()
            # proxy = 'http://localhost:8888'
            # print(proxy)
            return (await self.download_straight(req, headers, proxy=proxy))
        except (ProxyFectchError, DownLoadError) as err:
            raise DownLoadError(err)
    
    async def download_straight(self, req, headers={}, proxy=None):
        """每次下载时，都要开启一个下载协程，session托管在下载协程中，session是有限的，下载协程是无限的"""
        # 不能仅仅根据data是否为空来判断POST\GET请求，有时候明明为post请求，偏偏data为空
        session = await self.idle_session_pool.get()
        try:
            # _log_req('%s %s' % (proxy, _format_req(req)))

            method = req[1]
            url = req[2]

            referer = req[3]
            if referer:
                headers['Referer'] = referer
            
            payload = req[4]
            
            if method == 'POST':
                async with session.post(url=url, headers=headers, data=payload, proxy=proxy) as resp:
                    # print(url, resp.status)
                    if resp.status == 200:
                        source = await resp.text()
                        # print(source)
                    else:
                        # 下载时发生httpstatus异常
                        raise DownLoadError(url)
            else:
                async with session.get(url=url, headers=headers, proxy=proxy) as resp:
                    if resp.status == 200:
                        source = await resp.text()
                    else:
                        # 下载时发生httpstatus异常
                        raise DownLoadError(url)

            return source
        except (aiohttp.ClientError, asyncio.TimeoutError, UnicodeDecodeError) as err:
            # 下载时发生网络异常
            if session.closed:
                # 如果这个 session 已经关闭，那么重新创建一个session
                session = aiohttp.ClientSession(headers=DEFAULT_REQUEST_HEADERS, timeout=aiohttp.ClientTimeout(60), cookie_jar=aiohttp.DummyCookieJar())
            raise DownLoadError(err)
        finally:
            # 此协程退出或耗尽时，都必须释放所占有的session
            await self.idle_session_pool.put(session)
    
    async def close_sessions(self):
        """退出时，由下载管理器将所有session关闭"""
        while True:
            if self.idle_session_pool.empty():
                break
            session = await self.idle_session_pool.get()
            await session.close()
        if self.proxy_used:
            await self.proxyDirector.session.close()

class ProxyDirector():
    def __init__(self):
        self.session = aiohttp.ClientSession(headers=DEFAULT_REQUEST_HEADERS, timeout=aiohttp.ClientTimeout(60), cookie_jar=aiohttp.DummyCookieJar())
        self.proxy_ip_pool = Queue(maxsize=PROXY_TIMES_SIZE)
        self.semaphore = asyncio.Semaphore()
        self.proxy_key_url = PROXY_KEY_URL
        self.proxy_times_size = PROXY_TIMES_SIZE
    
    async def get_proxy_ip(self):
        """此处要对代理ip池为空做两次判断，那一开始来说，n个协程都来取代理ip，都发现池子是空的，都去取semaphore，必然只有一个协程获取到semaphore
        那个幸运协程从网上下载代理ip并存放到池子里后，释放semaphore，拿到一个代理ip后耗尽其协程。
        其余协程等待到semaphore后，再一次判断池子是否为空，发现不为空，不再到网上下载代理ip，而是直接从池子中获取到已有代理ip后耗尽协程。
        
        对于get_proxy_ip() coroutine来说，一次获取，要么获取到合法的代理ip，要么抛出代理ip获取异常ProxyFetchError。

        对于从网上下载代理ip发生异常，以及获取的到代理ip不符合规范，都需要抛出异常。
        
        无论是正常结束，还是发生异常情况，退出前都需要释放semaphore，不行就由别人来上。"""
        if self.proxy_ip_pool.empty():
            #此信号标的作用为，保证一次只有一个协程来根据key下载代理ip
            await self.semaphore.acquire()
            # 利用上下文管理器管理信号标，保证退出此上下文后都必须release
            # async with (await self.semaphore):
            try:
                if self.proxy_ip_pool.empty():
                    async with self.session.get(self.proxy_key_url) as resp:
                        if resp.status == 200:
                            proxy_ip = await resp.text()
                            # print(proxy_ip)

                            mo = PROXY_IP_PATTERN.search(proxy_ip)
                            if mo:
                                for _ in range(0, self.proxy_times_size):
                                    await self.proxy_ip_pool.put('http://%s' % mo.group(0))
                            
                            else:
                                # 下载代理ip获得正常响应，但是代理ip不合法，比如欠费导致
                                # print('zzz')
                                # self.semaphore.release()
                                raise ProxyFectchError(self.proxy_key_url)
                        
                        else:
                            #下载代理ip请求获得相应，但是响应httpstatus有误
                            # print('yyy')
                            # self.semaphore.release()
                            raise ProxyFectchError(self.proxy_key_url)
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                #下载代理ip时网络异常
                # print('xxx')
                if self.session.closed:
                    self.session = aiohttp.ClientSession(headers=DEFAULT_REQUEST_HEADERS, timeout=aiohttp.ClientTimeout(60), cookie_jar=aiohttp.DummyCookieJar())
                raise ProxyFectchError(err)
            finally:
                #发生异常，一定要释放此信号量，如果某个协程在请求代理ip资源时发生异常，没有释放此信号量。那么所有的协程将阻塞在获取本信号量处
                self.semaphore.release()

        return (await self.proxy_ip_pool.get())

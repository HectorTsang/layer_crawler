# -*- coding: utf-8 -*-
import time
import asyncio
from functools import partial

from lxml import etree

with open('config.xml', 'rb') as fp:
    el_config = etree.XML(fp.read())

    ACTIVITY_INTERVAL = int(el_config.xpath('./activity/interval/text()')[0])*60

    COROUTINE_COUNT = int(el_config.xpath('./work/concurrency/text()')[0])
    CLIENT_SESSION_COUNT = int(el_config.xpath('./downloader/client_sesseion_count/text()')[0])
    # 抓取时，最大协程选取设置中的‘coroutine_count client_session_count’较小值
    COROUTINE_COUNT = min(COROUTINE_COUNT, CLIENT_SESSION_COUNT)
del el_config, fp, CLIENT_SESSION_COUNT

class BaseEngine():
    def __init__(self):
        self.activity_interval = ACTIVITY_INTERVAL
        self.coroutine_semaphore = asyncio.Semaphore(COROUTINE_COUNT)
        self.scheduler = None
        self.downloaderDirector = None
        self.pipelineDirector = None

    async def activity(self, new_collection=False):
        """一次采集活动的过程"""
        # 初始化downloaderDirector，创建clientsession
        await self.downloaderDirector.init_idle_session_pool()
        # 初始化，创建 cnx,cursor
        self.pipelineDirector.build_cnx_cursor()
        
        # 初始化scheduler的unvisited_reqs
        if not new_collection:
            # 如果不是重新抓取，则从快照中保存的reqs信息初始化到scheduler的unvisited_reqs中
            self.scheduler.import_4file2qs()
        else:
            # 如果是重新抓取，则将首页req0初始化到scheduler的unvisited_reqs中
            self.create_new_crawl_tables()
            self.scheduler.init_unvisited_reqs_in_new_collection()
        
        print_event_time('[activity 一次采集活动开始 ]')
        self.scheduler.print_2colls_status()
        # 每从scheduler中获取一个req，便由asyncio框架排定一个针对该req抓取提取入库的协程[coro: crawl_one_req(req)]，由此实现并发
        while True:
            if self.scheduler.check_2colls_empty():
                # 退出机制，若scheduler维护的未访问、正在访问两个集合中都没有req，则说明采集协程全部结束，则本采集任务结束
                break
            else:
                try:
                    # 此处在此work协程中设置暂停点，相当重要！为后面的采集协程提供排期的机会，如果此循环一直循环执行不暂停，后面的采集协程即使让asyncio框架排期，也没有无期可排
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    # 若在此暂停点捕获到cancel操作，直接跳出循环
                    print_event_time('[activity 本次采集活动中止，不再从 scheduler 中获取 req 给 事件循环排期 ]')
                    return

                # 从scheduler中取出一个未访问req，并自动将此req标记到正在访问集合中
                req = self.scheduler.fetch_unvisited_req()
                # 若从scheduler中成功获取一个req，则异步暂停等待获取一个协程信号量，并创建一个采集单个req的协程，并将此协程交由asyncio框架排期，信号量的释放在采集协程结束后释放
                if req:
                    try:
                        await self.coroutine_semaphore.acquire()
                    except asyncio.CancelledError:
                        # 若在此暂定点捕获cancel操作，获取到的req已经从 unvisited_reqs 中拿出来，并在 visiting_reqs 中做标记了
                        # 但是还没有为这个 req 创建协程，并交给 asyncio 框架排期
                        # 那么这个 req 就会一直在 visiting_reqs 中，不利于退出 exit 判断，所以要将这个 req 返回给 unvisited_reqs，并从 visiting_reqs 中擦除标记
                        self.scheduler.input_unvisited_req(req)
                        self.scheduler.erasure_recorded_visiting_req(req)
                        print_event_time('[activity 本次采集活动中止，不再从 scheduler 中获取 req 给 事件循环排期 ]')
                        return

                    asyncio.ensure_future(self.work_coro_func(req))
                else:
                    #若scheduler中未访问集合中暂时没有req，则继续循环获取req
                    continue
        print_event_time('[activity 本次采集活动结束 ]')
        self.scheduler.print_2colls_status()

    async def cleanup_activity(self, activity_task):
        """
        退出操作包括：
        0. 首先将 work 任务停止，使其不再为 asyncio 框架提供协程
        1. scheduler 将所持有的 req 集合保存快照
        2. asyncio 框架所排定的所有 task 结束，宁愿先保存快照，即使重复也不遗漏
        3. downloaderDirector 中所持有的所有 clientsession 关闭
        4. pipeline 中的cnx关闭
        """
        activity_task.cancel()

        print_event_time('<cleanup 正在保存待采集队列快照>')
        self.scheduler.export_4qs2file()
        
        print('<cleanup 正在等待剩下的采集作业结束，以便关闭 downloader 之 session ... >')
        while True:
            """当没有正在访问的 req 时，表示采集剩下的采集程序全部运行完毕"""
            await asyncio.sleep(0.1)
            if self.scheduler.check_visiting_reqs_empty():
                break
        print_event_time('<cleanup 剩下的采集作业结束 >')
        self.scheduler.print_2colls_status()

        await self.downloaderDirector.close_sessions()
        self.pipelineDirector.close_cnx_cursor()

    def drive_activity_and_cleanup(self, new_collection=False):
        try:
            eventloop = asyncio.get_event_loop()
            activity_task = asyncio.ensure_future(self.activity(new_collection))
            eventloop.run_until_complete(activity_task)
        except KeyboardInterrupt as err:
            raise err
        finally:
            # 退出操作
            eventloop.run_until_complete(self.cleanup_activity(activity_task))
    
    async def work_coro_func(self, req):
        """需要用到的作业协程函数，本类中为空，用于后边具体派生到作业类型的引擎时，重写此真正的作业协程"""
        pass
    
    def recover_crawl_status(self):
        """在派生的具体引擎中重写，用于每次采集活动结束后，在数据库中进行的抓取状态校验，作业不同，校验的方式也不同"""
        pass
    
    def create_new_crawl_tables(self):
        """在派生的具体引擎中重写，用于每次活动开始前，如果全新抓取判断为确定，存档前面的抓取表，并创建新的抓取表"""
        pass
    
    def start(self, new_collection=False):
        """引擎开启后, 循环驱动执行一次次地采集活动
        每次采集活动之间有间隔；每次采集活动结束后都要在数据库中校验采集状态
        每盘引擎被开启后，只对第一次采集活动做是否是新采集活动的判断，后面的采集活动默认从中断点采集
        引擎一旦被开启，就不结束，循环间隔就是采集活动的间隔策略，如每隔多久进行一次采集活动
                                每天、每周一个时刻进行一次采集活动，可以设置，但是这样的话，每天只有一次采集活动，一次活动不一定采集完整"""
        print('{engine 采集引擎已经启动... }')
        try:
            self.drive_activity_and_cleanup(new_collection)
            while True:
                self.recover_crawl_status()
                print('<!------- 本次采集活动后，根据采集结果重置 scheduler 完成 ------->')
                time.sleep(self.activity_interval)
                self.drive_activity_and_cleanup()
        except KeyboardInterrupt:
            # 引擎启动后始终都是用的一个 asyncio 框架默认的事件循环，这个 eventloop 为所有 activity 公用，所以只有在引擎中止时关闭
            asyncio.get_event_loop().close()
            print('{engine 采集引擎已经中止... }')

def print_event_time(tip):
    """打印事件及发生时间"""
    print('%s于%s' % (tip,time.strftime('%Y-%m-%d %H:%M:%S')))

def log(fi, layer, step, err):
    """抓取日志"""
    with open('log\\{0}.log'.format(fi), 'a', encoding='utf-8') as fp:
        fp.write('[ %s ] < %s > -- %s -- %s\n' % (time.strftime('%Y-%m-%d %H:%M:%S'), layer, step, err))

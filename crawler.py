# -*- coding: utf-8 -*-
import time
import asyncio
from functools import partial

from layer_crawler.scheduler import Scheduler
from layer_crawler.downloader import DownloaderDirector, DownLoadError
from spider import MacroStatisticsSpider, Layer0BatchlistSpider, Layer1BaseinfoSpider, Layer2BuildingBranchlistSpider, Layer3BranchRoomStateSpider, Layer4RoomAttriSpider, ExtractError
from item_pipeline import ItemPipelineDirector, PipelineError
from layer_crawler.engine import BaseEngine, log, COROUTINE_COUNT, ACTIVITY_INTERVAL

from lxml import etree

class MacroStatisticsEngine(BaseEngine):
    def __init__(self, work_category):
        BaseEngine.__init__(self)
        self.scheduler = Scheduler(work_category)
        self.downloaderDirector = DownloaderDirector()
        self.pipelineDirector = ItemPipelineDirector()
    
    async def work_coro_func(self, req):
        layer = req[0]        
        try:
            source = await self.downloaderDirector.download(req)        
            
            try:
                room_usegroup_items = MacroStatisticsSpider.extract_room_usegroup_macro_statistics(source)
                self.pipelineDirector.insert_macro_room_usegroup_items(room_usegroup_items)
            except ExtractError:
                pass
            
            try:
                residence_areagroup_items = MacroStatisticsSpider.extract_residence_areagroup_macro_statistics(source)
                self.pipelineDirector.insert_macro_residence_areagroup_items(residence_areagroup_items)
            except ExtractError:
                pass
            
            if layer == 0:
                req0 = req
                try:
                    req1s = MacroStatisticsSpider.extract_create_macro_cityarea_req1(req0, source)
                    self.scheduler.input_unvisited_req_list(req1s)
                except ExtractError:
                    pass
        except (DownLoadError, PipelineError) as err:
            self.scheduler.input_unvisited_req(req)
            log(layer, err.__class__, err)
        finally:
            self.scheduler.erasure_recorded_visiting_req(req)
            self.coroutine_semaphore.release()
    
    def recover_crawl_status(self):
        pass

class ProjectEngine(BaseEngine):
    def __init__(self, work_category):
        BaseEngine.__init__(self)
        self.scheduler = Scheduler(work_category)
        self.downloaderDirector = DownloaderDirector()
        self.pipelineDirector = ItemPipelineDirector()
    
    async def work_coro_func(self, req):
        layer = req[0]
        # print(layer)
        try:
            source = await self.downloaderDirector.download(req)
            # print(source)
            if layer == 0:
                req0 = req
                try:
                    batch_info = Layer0BatchlistSpider.extract_batch_info(source)
                    if batch_info:
                        req1_base = Layer0BatchlistSpider.create_base_req1s(req0, batch_info)

                        req1_batch = Layer0BatchlistSpider.create_batch_req1s(req0, batch_info)
                        if req0[4]:
                            batch_item = Layer0BatchlistSpider.create_batch_all_item(req0[4][2][1], batch_info, req1_batch, req1_base)
                        else:
                            batch_item = Layer0BatchlistSpider.create_batch_all_item('1', batch_info, req1_batch, req1_base)
                        # print(batch_item)
                        self.pipelineDirector.insert_batch_items(batch_item)
                        self.scheduler.input_unvisited_req_list(req1_base)
                    else:
                        pass
                except ExtractError as err:
                    log(layer, err.__class__, err)
                
                try:
                    req0_next_page = Layer0BatchlistSpider.extract_create_next_page_req0(req0, source)
                    if req0_next_page:
                        self.scheduler.input_unvisited_req(req0_next_page)
                    else:
                        pass
                except ExtractError as err:
                    log(layer, err.__class__, err)
            elif layer == 1:
                req1 = req
                try:
                    base_info = Layer1BaseinfoSpider.extract_base_info(source)
                    if base_info:
                        base_item = Layer1BaseinfoSpider.create_baseinfo_item(req1, base_info)
                        self.pipelineDirector.insert_base_item(base_item)
                    else:
                        pass
                except ExtractError as err:
                    log(layer, err.__class__, err)
                
                try:
                    building_info = Layer1BaseinfoSpider.extract_all_building(source)
                    if building_info:
                        req2_building = Layer1BaseinfoSpider.create_building_req2s(req1, building_info)
                        building_item = Layer1BaseinfoSpider.create_building_item(req1, building_info, req2_building)
                        self.pipelineDirector.insert_building_items(building_item)
                        self.scheduler.input_unvisited_req_list(req2_building)
                    else:
                        pass
                except ExtractError as err:
                    log(layer, err.__class__, err)
            elif layer == 2:
                req2 = req
                req3_branch_after = None
                try:
                    branch_info = Layer2BuildingBranchlistSpider.extract_branch_info(source)
                    if branch_info:
                        req3_branch = Layer2BuildingBranchlistSpider.create_branch_req3s(req2, branch_info)
                        req3_branch_after = Layer2BuildingBranchlistSpider.create_branch_req3s_after(req3_branch)
                        branch_item = Layer2BuildingBranchlistSpider.create_branch_item(req2, branch_info, req3_branch)
                        self.pipelineDirector.insert_branch_items(branch_item)
                    else:
                        pass
                except ExtractError as err:
                    log(layer, err.__class__, err)
                
                try:
                    req3 = req3_branch[0]
                    room_state_info_1st_branch = Layer2BuildingBranchlistSpider.extract_1st_branch_room_state(source)
                    if room_state_info_1st_branch:
                        req4_roomattri = Layer2BuildingBranchlistSpider.create_1st_branch_room_req4s(req3, room_state_info_1st_branch)
                        room_state_irem_1st_branch = Layer2BuildingBranchlistSpider.create_1st_branch_room_state_item(req3, room_state_info_1st_branch, req4_roomattri)
                        self.pipelineDirector.insert_roomstate_items(room_state_irem_1st_branch)
                    else:
                        pass
                except (ExtractError, UnboundLocalError) as err:
                    log(layer, err.__class__, err)
                
                if req3_branch_after:
                    self.scheduler.input_unvisited_req_list(req3_branch_after)

            elif layer == 3:
                req3 = req
                try:
                    room_state_info = Layer3BranchRoomStateSpider.extract_room_state_info(source)
                    if room_state_info:
                        req4_roomattri = Layer3BranchRoomStateSpider.create_room_req4s(req3, room_state_info)
                        roomstate_item = Layer3BranchRoomStateSpider.create_room_item(req3, room_state_info, req4_roomattri)
                        self.pipelineDirector.insert_roomstate_items(roomstate_item)
                    else:
                        pass
                except ExtractError as err:
                    log(layer, err.__class__, err)
        except (DownLoadError, PipelineError) as err:
            self.scheduler.input_unvisited_req(req)
            log(layer, err.__class__, err)
        finally:
            self.scheduler.erasure_recorded_visiting_req(req)
            self.coroutine_semaphore.release()
    
    def recover_crawl_status(self):
        try:
            self.pipelineDirector.build_cnx_cursor()

            self.pipelineDirector.call_proc('gen_req_md5')

            syntax_select_uncrawl_batch_on_base = 'select distinct bt0.req1_base from cn_gov_sz_zjj_batch bt0 left join cn_gov_sz_zjj_base bs0 on bt0.req1_base_md5 = bs0.req1_base_md5 where bs0.id is null'
            syntax_select_uncrawl_batch_on_building = 'select distinct bs0.req1_base from cn_gov_sz_zjj_base bs0 left join cn_gov_sz_zjj_building bu0 on bs0.req1_base_md5 = bu0.req1_base_md5 where bu0.id is null'
            syntax_select_uncrawl_building = 'select distinct bu0.req2_building from cn_gov_sz_zjj_building bu0 left join cn_gov_sz_zjj_branch br0 on bu0.req2_building_md5 = br0.req2_building_md5 where br0.id is null'
            syntax_select_uncrawl_branch = 'select distinct br0.req3_branch from cn_gov_sz_zjj_branch br0 left join cn_gov_sz_zjj_room_state rs0 on br0.req3_branch_md5 = rs0.req3_branch_md5 where rs0.id is null'

            uncrawl_batch_on_base = self.pipelineDirector.select(syntax_select_uncrawl_batch_on_base)
            uncrawl_batch_on_building = self.pipelineDirector.select(syntax_select_uncrawl_batch_on_building)
            uncrawl_building = self.pipelineDirector.select(syntax_select_uncrawl_building)
            uncrawl_branch = self.pipelineDirector.select(syntax_select_uncrawl_branch)

            uncrawl_batch_on_base = [eval(i[0]) for i in uncrawl_batch_on_base]
            uncrawl_batch_on_building = [eval(i[0]) for i in uncrawl_batch_on_building]
            uncrawl_building = [eval(i[0]) for i in uncrawl_building]
            uncrawl_branch = [eval(i[0]) for i in uncrawl_branch]

            self.scheduler.input_unvisited_req_list(uncrawl_batch_on_base)
            self.scheduler.input_unvisited_req_list(uncrawl_batch_on_building)
            self.scheduler.input_unvisited_req_list(uncrawl_building)
            self.scheduler.input_unvisited_req_list(uncrawl_branch)

            self.scheduler.export_4qs2file()
        finally:
            self.pipelineDirector.close_cnx_cursor()
    
    def create_new_crawl_tables(self):
        try:
            self.pipelineDirector.call_proc('create_new_grab_tables')
        except PipelineError:
            pass

class RoomattriEngine(BaseEngine):
    def __init__(self, work_category):
        BaseEngine.__init__(self)
        self.scheduler = Scheduler(work_category)
        self.downloaderDirector = DownloaderDirector()
        self.pipelineDirector = ItemPipelineDirector()
    
    async def work_coro_func(self, req):
        pass
    
    def recover_crawl_status(self):
        pass

def main():
    el_config = etree.parse('config.xml')
    total_work_category = el_config.xpath('/config/scheduler/category/name/text()')
    print('<! 本次并发量为：%s>' % COROUTINE_COUNT)
    print('<! 每次采集活动间隔：%s s>' % ACTIVITY_INTERVAL)
    print('本采集为 %s ，请选择要进行的采集作业类别：' % ('深圳市房地产信息系统'))
    id_work_ref = dict(zip(range(len(total_work_category)), total_work_category))
    for item in id_work_ref.items():
        print('%s: %s' % (item[0], item[1]))
    while True:
        choose = input('我选择的采集作业类别序号是 >>> ')
        try:
            work_category = id_work_ref[int(choose)]
            print('*** %s ***\n' % work_category)
            break
        except (ValueError, KeyError):
            print('请输入正确的采集作业类别序号：%s' % list(id_work_ref.keys()))
    
    print('采集引擎开启后，请选择首次采集活动的采集起点：')
    start_point_show = {'A':'全新采集', 'B':'从上次中断点继续'}
    for item in start_point_show.items():
        print('%s: %s' % (item[0], item[1]))
    while True:
        try:
            choose = input('请选择首次采集活动的采集起点 >>> ')
            choose = choose.upper()
            print('*** %s ***\n' % start_point_show[choose])
            break
        except KeyError:
            print('请输入正确的首次采集活动起点序号：%s' % list(start_point_show.keys()))
    new_collection = {'A':True, 'B':False}.get(choose)

    # print(work_category, new_collection)
    
    global log
    log = partial(log, work_category)

    Yngine = {'macro_statistics':MacroStatisticsEngine, 'project':ProjectEngine, 'roomattri':RoomattriEngine}.get(work_category)
    yngine = Yngine(work_category)
    yngine.start(new_collection)

if __name__ == '__main__':
    main()

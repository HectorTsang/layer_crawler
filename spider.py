# -*- coding: utf-8 -*-

import re
from copy import copy
from urllib.parse import urljoin

from lxml import etree
# from xml.etree import ElementTree as etree

"""
一个层级req --> 访问该层级req后得到的页面

mreq(0)：
    宏观统计主页req，访问后可从页面提取: 全市宏观统计信息item，分区宏观统计req(1)
mreq(1):
    分区宏观统计页面req，访问后可从页面提取: 分区宏观统计信息item

req0:
    预售证列表页面 req0，访问后可从页面提取：翻页下一页 req0；项目详情页面 req1；预售证细分详情页面 req1（暂时不抓取）；预售证信息item
req1:
    项目详情页面 req1，访问后可从页面提取：项目详情item；楼栋信息item；楼栋进入页面 req2
req2:
    楼栋页面 req2，访问后可从页面提取：楼栋下除开第一个的座号 req3；楼栋下所有座号item；楼栋下第一个座号销控表房间的 req4；楼的下第一个座号销控表房间状态item
req3:
    楼栋座号页面 req3，访问后可从页面提取：座号下销控表房间 req4；座号下销控表房间状态item
req4:
    房间属性页面 req4，访问后可从页面提取：房间属性item

！！！为了维护 req 1-2-3 之 预售证-楼栋-房间 的关系，需要在预售证item中添加其req(1), 方便楼栋关联上父级预售证，同理楼栋item中也需要添加其req(2)
"""
class ExtractError(Exception):
    """数据提取异常，一般由于网页元素结构或内容在提取过程中，发生提取模板以外的情况
    按模板提取，利用捕获异常来处理，简省许多 if 判断"""
    def __init__(self, err):
        self.err = err

# 宏观统计数据提取 ****************************************************************************************************************
class MacroStatisticsSpider():
    @staticmethod
    def extract_create_macro_cityarea_req1(req0, source):
        """提取并生成各城区请求req1"""
        try:
            html = etree.HTML(source)
            # print(html, type(html))

            # 一、单个城区的 post 请求的 payload
            postdict = {}
            # 提取 post 请求的input name-value，隐式input
            pattern = re.compile(r'Sys\.WebForms\.PageRequestManager\._initialize\([\'"](\w*)[\'"].*?Sys\.WebForms\.PageRequestManager\.getInstance\(\)\._updateControls\(\[[\'"](\w*)[\'"]', re.S)
            # pattern = re.compile(r'Sys\.WebForms\.PageRequestManager\._initialize\([\'"](\w*)[\'"]', re.S)
            mo = pattern.search(source)
            postdict[mo.group(1)] = mo.group(2)
            
            # 提取 post 请求的input name-value，显示input
            el_input_all_postdata = html.xpath('//input')
            for el_input_postdata in el_input_all_postdata:
                input_name = el_input_postdata.xpath('./@name', smart_strings=False)[0]
                input_value = el_input_postdata.xpath('./@value', smart_strings=False)[0]
                postdict[input_name] = input_value

            # 追加一个空的 input
            postdict[''] = ''
            # print(postdict)

            # 二、提取出所有城区的 keyid，并生成所有城区post请求的 req1
            el_div_recordlink = html.xpath('//div[contains(@class, "recordLink")]')[0]
            el_a_current_cityarea = el_div_recordlink.xpath('./a[@style="color:Red;"]')[0]
            current_cityarea_name = el_a_current_cityarea.xpath('./text()', smart_strings=False)[0]

            req1s = []
            referer = req0[2]
            url = html.xpath('//form[@id="Form1"]/@action', smart_strings=False)[0]
            url = urljoin(referer, url)

            el_a_all_cityarea = el_div_recordlink.xpath('./a[contains(@href, "javascript:__doPostBack")]')
            for el_a_cityarea in el_a_all_cityarea:
                payload = copy(postdict)
                cityarea_name = el_a_cityarea.xpath('./text()', smart_strings=False)[0]
                cityarea_keyid = el_a_cityarea.xpath('./@href', smart_strings=False)[0]
                cityarea_keyid = eval(cityarea_keyid.strip("javascript:__doPostBack"))
                if cityarea_name != current_cityarea_name:
                    payload['__EVENTTARGET'] = cityarea_keyid[0]
                    payload['__EVENTARGUMENT'] = cityarea_keyid[1]
                    payload['scriptManager2'] += '|%s' % cityarea_keyid[0]
                    payload = tuple(payload.items())
                    # print(payload)
                    req1s.append((1, 'POST', url, referer, payload))

            return req1s
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def extract_room_usegroup_macro_statistics(source):
        """提取出 所有房间 按用途分组 的宏观统计信息"""
        try:
            pattern = re.compile(r'<div[^>]*?recordlistBox fix[^>]*?>.*?商品房成交信息.*?</div>', re.S)
            mo = pattern.search(source)
            el_div_room_usegroup = etree.HTML(mo.group(0))
            # print(el_div_room_usegroup)
            
            statistics_date = el_div_room_usegroup.xpath('//span[@id="ctl03_lblCurTime2"]/text()', smart_strings=False)[0]
            statistics_cityarea = el_div_room_usegroup.xpath('//span[@id="ctl03_lbldistrict2"]/text()', smart_strings=False)[0]
            # print(statistics_date, statistics_cityarea)

            room_usegroup_macro_statistics = []
            el_tr_all_group = el_div_room_usegroup.xpath('//tr')[1:]
            for el_tr_group in el_tr_all_group:
                statistics = el_tr_group.xpath('./td/span/text()', smart_strings=False)
                if len(statistics) == 5:
                    room_usegroup_macro_statistics.append(tuple([statistics_date, statistics_cityarea]+statistics))
            return room_usegroup_macro_statistics
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def extract_residence_areagroup_macro_statistics(source):
        """提取出 仅限住宅 按面积分组 的宏观统计信息"""
        try:
            pattern = re.compile(r'<div[^>]*?recordlistBox fix[^>]*?>.*?商品住房按面积统计成交信息.*?</div>', re.S)
            mo = pattern.search(source)
            el_div_residence_areagroup = etree.HTML(mo.group(0))

            statistics_date = el_div_residence_areagroup.xpath('//span[@id="ctl03_lblCurTime5"]/text()', smart_strings=False)[0]
            statistics_cityarea = el_div_residence_areagroup.xpath('//span[@id="ctl03_lbldistrict5"]/text()', smart_strings=False)[0]

            residence_areagroup_macro_statistics = []
            el_tr_all_group = el_div_residence_areagroup.xpath('//tr')[1:]
            for el_tr_group in el_tr_all_group:
                statistics = el_tr_group.xpath('./td/span/text()', smart_strings=False)
                if len(statistics) == 3:
                    residence_areagroup_macro_statistics.append(tuple([statistics_date, statistics_cityarea]+statistics))
            return residence_areagroup_macro_statistics

        except Exception as err:
            raise ExtractError(err)

# 项目信息数据提取 ****************************************************************************************************************
# 从 预售证列表翻页 页面提取数据 -------------------------------
class Layer0BatchlistSpider():
    @staticmethod    
    def extract_create_next_page_req0(req0, source):
        """提取预售证列表翻页下一页信息，两个模板:
        非尾页模板，下一页 <a> 有效，含 href
        尾页模板，下一页 <a> 无效，不含 href"""
        try:
            el_html = etree.HTML(source)
            el_a_next_page = el_html.xpath('//div[@id="AspNetPager1"]//a[text()=">"]')[0]
            next_page_post_data = el_a_next_page.xpath('./@href', smart_strings=False)
            if next_page_post_data:
                next_page_post_data = next_page_post_data[0]
                next_page_post_data = eval(next_page_post_data.strip('javascript:__doPostBack'))
                # print(next_page_post_data)

                postdict = {}
                # 提取 post 请求的input name-value，隐式input
                pattern = re.compile(r'Sys\.WebForms\.PageRequestManager\._initialize\([\'"](\w*)[\'"].*?Sys\.WebForms\.PageRequestManager\.getInstance\(\)\._updateControls\(\[[\'"](\w*)[\'"]', re.S)
                # pattern = re.compile(r'Sys\.WebForms\.PageRequestManager\._initialize\([\'"](\w*)[\'"]', re.S)
                mo = pattern.search(source)
                postdict[mo.group(1)] = mo.group(2)

                el_input_all_postdata = el_html.xpath('//input[@type="hidden"]')
                for el_input_postdata in el_input_all_postdata:
                    input_name = el_input_postdata.xpath('./@name', smart_strings=False)[0]
                    input_value = el_input_postdata.xpath('./@value', smart_strings=False)[0]
                    postdict[input_name] = input_value
                    # print(input_name)
                    # print(input_value)
                
                el_input_all_postdata1 = el_html.xpath('//input[@type="text"]')
                for el_input_postdata1 in el_input_all_postdata1:
                    input_name = el_input_postdata1.xpath('./@name', smart_strings=False)[0]
                    postdict[input_name] = ''
                    # print(input_name)
                
                el_select_ddlpagecount = el_html.xpath('//select[@name="ddlPageCount"]')[0]
                optionname = el_select_ddlpagecount.xpath('./@name', smart_strings=False)[0]
                optionvalue = el_select_ddlpagecount.xpath('./option[last()]/@value', smart_strings=False)[0]
                postdict[optionname]=optionvalue
                
                postdict[''] = ''
                postdict['scriptManager2'] += ('|%s' % next_page_post_data[0])
                postdict['__EVENTTARGET'] = next_page_post_data[0]
                postdict['__EVENTARGUMENT'] = next_page_post_data[1]
                
                url = el_html.xpath('//form[@id="Form1"]/@action', smart_strings=False)[0]
                referer = req0[2]
                url = urljoin(referer, url)
                
                return (0, 'POST', url, referer, tuple(postdict.items()))

            return
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def extract_batch_info(source):
        """从预售证列表中提取出预售证信息，一个模板"""
        try:
            el_html = etree.HTML(source)
            el_table_all_batch = el_html.xpath('//div[@id="updatepanel2"]//table')[0]

            all_batch_info = []
            for el_tr_batch in el_table_all_batch.xpath('./tr[@class]'):
                full_info = el_tr_batch.xpath('.//@href', smart_strings=False)
                full_info.append(el_tr_batch.xpath('./td[1]/text()', smart_strings=False)[0].strip())
                full_info.append(el_tr_batch.xpath('./td[2]/a/text()', smart_strings=False)[0].strip())
                full_info.append(el_tr_batch.xpath('./td[3]/a/text()', smart_strings=False)[0].strip())
                full_info.append(el_tr_batch.xpath('./td[4]/text()', smart_strings=False)[0].strip())
                full_info.append(el_tr_batch.xpath('./td[5]/text()', smart_strings=False)[0].strip())
                full_info.append(el_tr_batch.xpath('./td[6]/text()', smart_strings=False)[0].strip())
                # full_info_tail = el_tr_batch.xpath('.//text()', smart_strings=False)
                # for info in full_info_tail:
                #     foo = info.strip()
                #     if foo:
                #         full_info.append(foo)
                all_batch_info.append(tuple(full_info))
            
            return all_batch_info
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def create_batch_req1s(req0, all_batch_info):
        """构建访问预售证详情页面的 req1 """
        referer = req0[2]
        req1s = []
        for batch_info in all_batch_info:
            req1s.append((1, 'GET', urljoin(referer, batch_info[0]), referer, None))
        return req1s
    
    @staticmethod
    def create_base_req1s(req0, all_batch_info):
        """构建访问项目详情页面的 req1"""
        referer = req0[2]
        req1s = []
        for batch_info in all_batch_info:
            req1s.append((1, 'GET', urljoin(referer, batch_info[1]), referer, None))
        return req1s
    
    @staticmethod
    def create_batch_all_item(req0, all_batch_info, batch_req1s, base_req1s):
        """构建入库的 batch_item """
        pipeline_item = []
        # print(len(info_list), len(reqy_list))
        for item, reqy, reqy1 in zip(all_batch_info, batch_req1s, base_req1s):
            foo = list(item)
            foo.insert(0, str(req0))
            foo.append(str(reqy))
            foo.append(str(reqy1))
            pipeline_item.append(tuple(foo))
        return pipeline_item

# 从 项目详情页面 提取信息 -----------------------------------
class Layer1BaseinfoSpider():
    
    @staticmethod
    def extract_base_info(source):
        """提取项目信息，模板只有一个"""
        try:
            el_html = etree.HTML(source)
            el_table_baseinfo = el_html.xpath('//table[@class="table ta-c table2 table-white"]')[0]
            el_tr_full_baseinfo = el_table_baseinfo.xpath('./tr[position()<14]')
            full_baseinfo_str = ''
            for el_tr_baseinfo in el_tr_full_baseinfo:
                full_baseinfo_str += etree.tostring(el_tr_baseinfo, encoding='unicode', method='html')
            # print(full_baseinfo_str)
            full_baseinfo_str = re.subn(r'\r|\n|\t| ', '', full_baseinfo_str)[0]
            full_baseinfo_str = re.subn(r'<div[^>]*?>|</div>|<tr[^>]*?>|</tr>|<br/?>', '', full_baseinfo_str)[0]
            full_baseinfo_str = re.subn(r'<td[^>]*?>', '<td>', full_baseinfo_str)[0]
            full_baseinfo_str = re.subn(r'<!--[^>]*?-->', '', full_baseinfo_str)[0]
            # print(full_baseinfo_str)
            full_baseinfo = re.findall(r'<td>([^<]*?)</td><td>([^<]*?)</td>', full_baseinfo_str)
            # full_baseinfo_filed = tuple(dict(full_baseinfo).keys())
            full_baseinfo_value = tuple(dict(full_baseinfo).values())
            # print(full_baseinfo_filed)
            # print(full_baseinfo_value)
            return full_baseinfo_value
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def extract_all_building(source):
        """提取所属楼栋信息，模板只有一个"""
        try:
            el_html = etree.HTML(source)
            el_table_building = el_html.xpath('//table[@class="table ta-c table2 table-white"]')[1]
            el_tr_all_building = el_table_building.xpath('./tr[position()>1]')
            
            all_building_info = []
            for el_tr_building in el_tr_all_building:
                i1 = el_tr_building.xpath('./td[1]/text()', smart_strings=False)[0]
                i1 = i1.strip()
                i2 = el_tr_building.xpath('./td[2]/text()', smart_strings=False)[0]
                i2 = i2.strip()
                i3 = el_tr_building.xpath('./td[3]/text()', smart_strings=False)[0]
                i3 = i3.strip()
                i4 = el_tr_building.xpath('./td[4]/text()', smart_strings=False)[0]
                i4 = i4.strip()
                href = el_tr_building.xpath('.//a/@href', smart_strings=False)[0]
                all_building_info.append((i1, i2, i3, i4, href))
            return all_building_info
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def create_building_req2s(req1, all_building_info):
        """构建访问楼栋下面座号表的 req2 """
        referer = req1[2]
        building_req2s = []
        for building_info in all_building_info:
            url = urljoin(referer, building_info[-1])
            building_req2s.append((2, 'GET', url, referer, None))
        return building_req2s

    @staticmethod
    def create_baseinfo_item(req1, base_info):
        """构建 项目详情 入库实体"""
        return recreate_pipeline_item_nohook(req1, base_info)

    @staticmethod
    def create_building_item(req1, all_building, req2s):
        """构建 楼栋详情 入库实体"""
        return recreate_pipeline_item(req1, all_building, req2s)

# 从 楼栋详情页面 提取信息 -----------------------------------
class Layer2BuildingBranchlistSpider():
    @staticmethod
    def extract_branch_info(source):
        """提取出楼栋座号信息"""
        try:
            el_html = etree.HTML(source)
            el_div_all_branch_info = el_html.xpath('//div[@id="divShowBranch"]')[0]
            
            all_branch = []
            el_a_all_branch_info = el_div_all_branch_info.xpath('./a')
            # print(el_a_all_branch_info)
            for el_a_branch_info in el_a_all_branch_info:
                branch_name = el_a_branch_info.xpath('./text()', smart_strings=False)[0].strip('[]')
                branch_href = el_a_branch_info.xpath('./@href', smart_strings=False)[0]
                all_branch.append((branch_href, branch_name))
            
            return all_branch
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def create_branch_req3s(req2, all_branch_info):
        """构建楼栋座号访问 req3 """
        referer = req2[2]
        req3s = []
        for branch_info in all_branch_info:
            req3s.append((3, 'GET', urljoin(referer, branch_info[0]), referer, None))
        return req3s
    
    @staticmethod
    def create_branch_req3s_after(all_branch_req3s):
        """构建除开第一个 branch ，剩余的座号需要放入 unvisited_reqs"""
        if len(all_branch_req3s) > 1:
            return all_branch_req3s[1:]

    @staticmethod
    def create_branch_item(req2, all_branch_info, branch_req3s):
        """构建楼栋座号 入库实体"""
        return recreate_pipeline_item(req2, all_branch_info, branch_req3s)
    
    pattern_room_state_info = re.compile(r'<div[^>]*?>房号：([^<>]*?)</div>.*?<div[^>]*?>.*?<a[^>]*?href=[\'"](.*?)[\'"][^>]*?>([^<>]*)', re.S)
    @classmethod
    def extract_1st_branch_room_state(cls, source):
        """提取第一个座号的房间销控表信息"""
        try:
            el_html = etree.HTML(source)
            # print(etree.tostring(el_html, encoding='unicode'))
            el_div_all_room_state = el_html.xpath('//div[@id="divShowList"]')[0]
            # print(el_div_all_room_state)

            all_floor_info = []
            el_td_all_floor = el_div_all_room_state.xpath('.//td[@rowspan]')
            for el_td_floor in el_td_all_floor:
                floor_name = el_td_floor.xpath('./div/text()', smart_strings=False)[0]
                floor_rowspan = int(el_td_floor.xpath('./@rowspan', smart_strings=False)[0])
                all_floor_info += [floor_name]*floor_rowspan
            # print(all_floor_info)

            all_floor_rooms = []
            el_tr_all_floor_room_state = el_div_all_room_state.xpath('./tr')
            for el_tr_floor_room_state in el_tr_all_floor_room_state:
                # print(el_tr_floor_room_state)
                floor_room_state_str = etree.tostring(el_tr_floor_room_state.xpath('./td[not(@rowspan)]')[0], encoding='unicode', method='html')
                floor_rooms = cls.pattern_room_state_info.findall(floor_room_state_str)
                all_floor_rooms.append(floor_rooms)
            # print(all_floor_rooms)

            all_room_info = []
            for floor_info, floor_rooms in zip(all_floor_info, all_floor_rooms):
                # print(floor_info)
                # print(floor_rooms)
                for room in floor_rooms:
                    room = list(room)
                    room.insert(0, floor_info)
                    all_room_info.append(tuple(room))
            return all_room_info
        except Exception as err:
            raise ExtractError(err)
    
    @staticmethod
    def create_1st_branch_room_req4s(req3, room_state_info):
        """构建访问房间详情的 req4 """
        referer = req3[2]
        room_req4s = []
        for room in room_state_info:
            room_req4s.append((4, 'GET', urljoin(referer, room[2]), referer, None))
        return room_req4s

    @staticmethod
    def create_1st_branch_room_state_item(req3, room_state_info, room_req4s):
        """构建房间状态入库实体"""
        return recreate_pipeline_item(req3, room_state_info, room_req4s)

# 从 楼栋座号详情页面 提取信息 -----------------------------------
class Layer3BranchRoomStateSpider():
    extract_room_state_info = Layer2BuildingBranchlistSpider.extract_1st_branch_room_state
    create_room_req4s = Layer2BuildingBranchlistSpider.create_1st_branch_room_req4s
    create_room_item = Layer2BuildingBranchlistSpider.create_1st_branch_room_state_item

# 从 房间详情页面 提取信息 -----------------------------------
class Layer4RoomAttriSpider():
    
    @classmethod
    def extract_room_attri_info(cls, source):
        try:
            el_html = etree.HTML(source)
            el_table_all_attri = el_html.xpath('//table[@class="table ta-c table2 table-white"]')[0]
            
            none_area_attri_str = ''
            el_tr_none_area_attri = el_table_all_attri.xpath('./tr[position()<4]')
            for el_tr_i in el_tr_none_area_attri:
                none_area_attri_str += etree.tostring(el_tr_i, encoding='unicode', method='html')
            
            presale_area_attri_str = etree.tostring(el_table_all_attri.xpath('./tr[5]')[0], encoding='unicode', method='html')

            completed_area_attri_str = etree.tostring(el_table_all_attri.xpath('./tr[7]')[0], encoding='unicode', method='html')

            none_area_attri = cls.__extract_from_td_structure(none_area_attri_str)
            presale_area_attri = cls.__extract_from_td_structure(presale_area_attri_str)
            completed_area_attri = cls.__extract_from_td_structure(completed_area_attri_str)

            # _log_spider(none_area_attri)
            # _log_spider(presale_area_attri)
            # _log_spider(completed_area_attri)

            return tuple(list(none_area_attri)+list(presale_area_attri)+list(completed_area_attri))
            
        except Exception as err:
            raise ExtractError(err)
    
    pattern_room_attri = re.compile(r'<td>([^<>]*)</td><td>([^<>]*)</td>')
    @classmethod
    def __extract_from_td_structure(cls, td_str):
        # 一个空格为nbsp不间断空格 \xa0，一个未普通空格 \x20
        td_str = re.subn(r'\r|\n|\t| | ', '', td_str)[0]
        td_str = re.subn(r'<td[^>]*>', '<td>', td_str)[0]
        td_str = re.subn(r'<!--[^>]*?-->', '', td_str)[0]
        room_attri = cls.pattern_room_attri.findall(td_str)
        return tuple(dict(room_attri).values())
    
    @staticmethod
    def create_room_attri_item(req4, room_attri_info):
        return recreate_pipeline_item_nohook(req4, room_attri_info)

# 根据 网页请求reqx、该网页提取的实体、要访问提取的实体详情的reqy 生成入库元组 *********************************************************
def recreate_pipeline_item(reqx, info_list, reqy_list):
    """
        /   tuple(info)   --- reqy
    reqx  ---  tuple(info)   --- reqy   ===> [(str(reqx), info*, str(reqy)), (str(reqx), info*, str(reqy)), (str(reqx), info*, str(reqy)), ...]
        \\  tuple(info)   --- reqy
    """
    pipeline_item = []
    # print(len(info_list), len(reqy_list))
    for item, reqy in zip(info_list, reqy_list):
        foo = list(item)
        foo.insert(0, str(reqx))
        foo.append(str(reqy))
        pipeline_item.append(tuple(foo))
    return pipeline_item

def recreate_pipeline_item_nohook(reqx, info):
    """reqx  ---  tuple(info)   ===> (str(reqx), info*)"""
    foo = list(info)
    foo.insert(0, str(reqx))
    return tuple(foo)

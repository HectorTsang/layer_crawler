# -*- coding: utf-8 -*-

from queue import Queue, Empty
import json
import pathlib
import copy

from lxml import etree

DEPTH, LAYER_DEEP_MAP, SNAPSHOT_UNVISITED_REQS_FILENAME, SNAPSHOT_VISITING_REQS_FILENAME, REQ_1ST= None, {}, None, None, None
def read_scheduler_config(work_category):
    """从配置文件中依次读取双集合深度、未访问请求快照文件名、中断点正在访问的请求快照文件名、以及重新抓取的情况下第一个请求req"""
    global DEPTH, LAYER_DEEP_MAP, SNAPSHOT_UNVISITED_REQS_FILENAME, SNAPSHOT_VISITING_REQS_FILENAME, REQ_1ST
    with open('config.xml', 'rb') as fp:
        config = etree.XML(fp.read())
        category = config.xpath('//scheduler/category[name=$name]', name=work_category)[0]

        DEPTH = int(category.xpath('./depth/text()', smart_strings=False)[0])
        SNAPSHOT_UNVISITED_REQS_FILENAME = category.xpath('./snapshot_unvisited_reqs_filename/text()', smart_strings=False)[0]
        SNAPSHOT_VISITING_REQS_FILENAME = category.xpath('./snapshot_visiting_reqs_filename/text()', smart_strings=False)[0]

        req_1st = category.xpath('./req_1st/text()', smart_strings=False)
        if req_1st:
            REQ_1ST = eval(req_1st[0])

        layer_deep_maps = category.xpath('./layer_deep_map/m')
        for m in layer_deep_maps:
            layer = m.xpath('./layer/text()', smart_strings=False)[0]
            deep = m.xpath('./deep/text()', smart_strings=False)[0]
            LAYER_DEEP_MAP[int(layer)] = int(deep)

# req格式：（所在业务层， 请求方法， url， refer， post请求的数据实体）
# layer, 0,1,2,3,..., 0层表示顶层页面，如西安房产网预售证列表翻页页面
# req: (layer, method, url, Referer, payload)

# 将一次采集作业所需要的深度，与业务req层分开来看，因为一个网站从业务上来说，层级是固定的。
# 但是采集的时候可能从多个方面去采集，如一个宏观统计采集深度只有2，房产项目-房间状态采集任务，深度达到3~4，而采集房间属性深度只有1。
# 所以将 scheduler 采集任务的 deep 与网站实际业务的 req_layer 层级解耦是有必要的

class InValidDeep(Exception):
    def __init__(self, layer):
        self.layer = layer

class Scheduler():
    """
    核心在于，维护着两个集合，一个放当前未采集的req，一个放当前正在采集的req，已经采集成功的req不再保留，没意义。
    处于采集过程中断后，再次开启采集后从中断点开始的需要，维持了上述一个正在采集req的集合。

    unvisited_reqs，由[Queue,Queue,Queue,...]维持，每层的queue没有容量限制，支持put/get操作。每次取req时，也不阻塞或异步暂停，采用.get_nowait()/.put_nowait()，所以不用asyncio.Queue，直接用Queue
    visiting_reqs, 由[Set,Set,Set,...]维持，每层的set也没有容量限制，支持add/discard(req)操作。
    两个集合容纳的元素量受并发量控制，这决定了其元素量不是无限增大的。

    采集中断时，需要保存正在采集任务当前状态的快照：unvisited_reqs, visiting_reqs。同时需要满足快照文件人可读，所以按json格式保存是一个可取的方案。
    json格式，两个集合的快照格式一致：
    {
        "deep_0":[(req0),(req0),(req0),...],
        "deep_1":[(req1),(req1),(req1),...],
        "deep_2":[],
        "deep_3":[(req3),(req3),(req3),...]
    }
    """
    def __init__(self, work_category):
        read_scheduler_config(work_category)
        self.depth = DEPTH
        # 需要维护一个 业务层级 layer ~ 某采集任务深度 deep 的映射，否则 req 不知道放入哪个深度的 集合中
        self.layer_deep_map = LAYER_DEEP_MAP

        self.unvisited_reqs = [Queue() for _ in range(DEPTH)]
        # Set中的元素必须可哈希
        self.visiting_reqs = [set() for _ in range(DEPTH)]

        self.snapshot_unvisited_reqs_filename = SNAPSHOT_UNVISITED_REQS_FILENAME
        self.snapshot_visiting_reqs_filename = SNAPSHOT_VISITING_REQS_FILENAME
    
    def input_unvisited_req(self, req):
        """往unvisited_reqs中添加一个req"""
        deep = self.layer_deep_map.get(req[0])
        if deep < self.depth and deep >= 0:
            self.unvisited_reqs[deep].put(req)
        else:
            raise InValidDeep(deep)
    
    def input_unvisited_req_list(self, req_list):
        """往unvisited_reqs中添加一个列表的req"""
        if req_list:
            deep = self.layer_deep_map.get(req_list[0][0])
            # print(deep)
            if deep < self.depth and deep >= 0:
                for req in req_list:
                    self.unvisited_reqs[deep].put(req)
            else:
                raise InValidDeep(deep)
    
    def fetch_unvisited_req(self):
        """从unvisited_reqs中取出一个req，采取层级深度优先，每层FIFO的策略，若各层reqs为空，则直接返回None，不阻塞等待或异步暂停"""
        try:
            req = None
            deep = self.depth-1

            while deep >= 0:
                q = self.unvisited_reqs[deep]
                
                try:
                    req = q.get_nowait()
                except Empty:
                    deep -= 1
                    continue
                else:
                    # else表示，没有异常发生时的操作。毕竟发生empty异常时，被上面except捕获，其他异常的话，没有捕获器，直接向上抛。只有不发生异常，才会进入else区
                    return req
            return
        finally:
            if req:
                # 将标记放在此处，防止程序刚好在此处中断，req从未访问队列集合中取出，还未记录到正在访问集合中，便中断，那么这个尚未采集的req既不在queues中也不在sets中
                self.record_visiting_req(req)
    
    def record_visiting_req(self, req):
        """往visiting_reqs中添加一个req标记。注意，打标记是自动的，在从unvisited_reqs中取req后，自动添加标记"""
        # print(req)
        self.visiting_reqs[self.layer_deep_map.get(req[0])].add(req)
    
    def erasure_recorded_visiting_req(self, req):
        """从visiting_reqs中擦除一个req标记。注意，擦除标记需要手动操作"""
        # 不管页面访问是否成功，都要擦除正在访问记录；若页面访问失败，再将此req记录插入到未访问队列集合中，以便再次访问。
        self.visiting_reqs[self.layer_deep_map.get(req[0])].discard(req)
    
    def init_unvisited_reqs_in_new_collection(self):
        """当全新采集时，向unvisited_reqs中录入第一个初始req"""
        self.input_unvisited_req(REQ_1ST)
    
    def check_2colls_empty(self):
        """检查两个集合是否为空，及所有层queue或set是否为空"""
        for qs in self.unvisited_reqs:
            if not qs.empty():
                return False
        for ss in self.visiting_reqs:
            if len(ss)>0:
                return False
        return True
    
    def check_visiting_reqs_empty(self):
        """检查是否还有正在访问的 req """
        for ss in self.visiting_reqs:
            if len(ss)>0:
                return False
        return True
    
    def export_4qs2file(self):
        """将两个集合的保存于快照文件中，如果没有req了，也要写入几个空集合xml到快照中
        python.tuple --> json.array, json.array --> python.list，而xml不涉及格式，全为纯文本"""
        self.print_2colls_status()
        el_root_unvisited_reqs = etree.Element('unvisited_reqs')
        for deep, qs in zip(range(self.depth), self.unvisited_reqs):
            el_queues = etree.SubElement(el_root_unvisited_reqs, 'queues', attrib={'deep':'{0}'.format(deep)})
            qsc = copy.copy(qs)
            while True:
                try:
                    req_str = str(qsc.get_nowait())
                    el_req = etree.SubElement(el_queues, 'req')
                    el_req.text = req_str
                except Empty:
                    break
        foo = etree.tostring(el_root_unvisited_reqs, encoding='utf-8', xml_declaration=True, pretty_print=True)
        with open(self.snapshot_unvisited_reqs_filename, 'wb') as fp:
            fp.write(foo)
        
        el_root_visiting_reqs = etree.Element('visiting_reqs')
        for deep, ss in zip(range(self.depth), self.visiting_reqs):
            el_sets = etree.SubElement(el_root_visiting_reqs, 'sets', attrib={'deep':'{0}'.format(deep)})
            ssc = copy.copy(ss)
            while True:
                try:
                    req_str = str(ssc.pop())
                    el_req = etree.SubElement(el_sets, 'req')
                    el_req.text = req_str
                except KeyError:
                    break
        bar = etree.tostring(el_root_visiting_reqs, encoding='utf-8', xml_declaration=True, pretty_print=True)
        with open(self.snapshot_visiting_reqs_filename, 'wb') as fp:
            fp.write(bar)
    
    def import_4file2qs(self):
        """将上次采集暂停时保存在快照文件中 未访问 req 与 正在访问 req 统统初始化到 未访问 req 集合中"""
        with open(self.snapshot_unvisited_reqs_filename, 'rb') as fp:
            el_unvisited_reqs = etree.XML(fp.read())
            for deep in range(self.depth):
                qs_deep = el_unvisited_reqs.xpath('//queues[@deep=$d]/req/text()', d=deep, smart_strings=False)
                qs_deep = [eval(req_str) for req_str in qs_deep]
                # print(qs_deep)
                # print(len(qs_deep))
                self.input_unvisited_req_list(qs_deep)
        
        with open(self.snapshot_visiting_reqs_filename, 'rb') as fp:
            el_visiting_reqs = etree.XML(fp.read())
            for deep in range(self.depth):
                ss_deep = el_visiting_reqs.xpath('//sets[@deep=$d]/req/text()', d=deep, smart_strings=False)
                ss_deep = [eval(req_str) for req_str in ss_deep]
                # print(ss_deep)
                # print(len(ss_deep))
                self.input_unvisited_req_list(ss_deep)


    def export_4qs2file_json(self):
        """
        将两个集合的保存于快照文件中，如果没有req了，也要写入几个空集合json到快照中
        程序运行结束，表示结束后 scheduler 状态
        """
        self.print_2colls_status()
        unvisited_reqs_dict = {'deep_{0}'.format(deep):[] for deep in range(self.depth)}
        visiting_reqs_dict = {'deep_{0}'.format(deep):[] for deep in range(self.depth)}
        # print(unvisited_reqs_dict, visiting_reqs_dict)

        for deep, qs in zip(unvisited_reqs_dict.keys(), self.unvisited_reqs):
            # 对一层queue进行浅复制，将原始队列里的元素引用到一个新的queue中
            # queue 即使浅复制，也会消耗原来 queue 的元素；深复制，TypeError: can't pickle _thread.lock objects
            # 所以 只有在结束时导出 reqs 集合到文件
            qsc = copy.copy(qs)
            # print(layer, qsc, qsc.qsize())
            while True:
                try:
                    unvisited_reqs_dict[deep].append(qsc.get_nowait())
                except Empty:
                    break
        # print(len(unvisited_reqs_dict[0]), len(unvisited_reqs_dict[1]), len(unvisited_reqs_dict[2]), len(unvisited_reqs_dict[3]))
        
        for deep, ss in zip(visiting_reqs_dict.keys(), self.visiting_reqs):
            # set 浅复制，不会消耗原来 set 的元素
            ssc = copy.copy(ss)
            # print(layer, ssc, len(ssc))
            while True:
                try:
                    visiting_reqs_dict[deep].append(ssc.pop())
                except KeyError:
                    break
        # print(len(visiting_reqs_dict[0]), len(visiting_reqs_dict[1]), len(visiting_reqs_dict[2]), len(visiting_reqs_dict[3]))
        
        with open(self.snapshot_unvisited_reqs_filename, 'w', encoding='utf-8') as fp:
            json.dump(unvisited_reqs_dict, fp)
        with open(self.snapshot_visiting_reqs_filename, 'w', encoding='utf-8') as fp:
            json.dump(visiting_reqs_dict, fp)

    def import_4file2qs_json(self):
        """两个快照文件只是暂存上次采集未访问以及正在访问的req，本次从暂存区获取后，还是保留着这两个文件数据
        上次正在采集的req，本次采集开启时，应当初始化到未采集队列中，重新采集，宁愿重复，不要遗漏
        """
        if pathlib.Path(self.snapshot_unvisited_reqs_filename).exists():
            with open(self.snapshot_unvisited_reqs_filename, 'r', encoding='utf-8') as fp:
                foo = json.load(fp)
                if foo:
                    self.input_4jsondecodedict_2unvisited_reqs_json(foo)
            # pathlib.Path(self.snapshot_unvisited_reqs_filename).unlink()

        if pathlib.Path(self.snapshot_visiting_reqs_filename).exists():
            with open(self.snapshot_visiting_reqs_filename, 'r', encoding='utf-8') as fp:
                bar = json.load(fp)
                if bar:
                    self.input_4jsondecodedict_2unvisited_reqs_json(bar)
            # pathlib.Path(self.snapshot_visiting_reqs_filename).unlink()
    
    def input_4jsondecodedict_2unvisited_reqs_json(self, dictreqs):
        """将req快照的json加载到程序形成dict对象后，将reqs插入到unvisited_reqs
        python(tuple, list) ---- encode/decode ---- json(array), 所以导出到json文件中，req是以json-array格式保存的，那么导入到程序中时，req便默认是list，所以需要tuple(req)操作
        """
        for _, deep_reqs in zip(dictreqs.keys(), dictreqs.values()):
            # print(layer_name)
            if deep_reqs:
                for req in deep_reqs:
                    payload = req[-1]
                    if payload and type(payload) is list:
                        payload = dict(payload)
                        payload = ((key, val) for key, val in zip(payload.keys(), payload.values()))
                        req[-1] = tuple(payload)
                    self.input_unvisited_req(tuple(req))
    
    def print_2colls_status(self):
        """格式化打印当前scheduler维持的两个集合的状态"""
        lz = 'deep'
        for deep in range(self.depth):
            lz += '%s%s' % (' '*10, deep)
        
        qz = 'ureqs'
        for qs in self.unvisited_reqs:
            qz += '%s%s' % (' '*10, qs.qsize())
        
        sz = 'ireqs'
        for ss in self.visiting_reqs:
            sz += '%s%s' % (' '*10, len(ss))
        print('%s\n%s\n%s\n%s' % ('-'*50, lz, qz, sz))

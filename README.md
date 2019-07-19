# layer_crawler
layer_crawler，集中管理网站采集多任务，并对每个任务以层级结构实现工程化采集。

### 设计背景
一个网站的页面结构，实际上是树结构。但也不是严格意义上的树结构，因为拿翻页来说，虽然下一页的父节点是上一页，但是翻页上下页是一个层级。同理，同一层级下打开的子页面也属于同一层级，如下图。

![image](https://github.com/HectorTsang/layer_crawler/blob/master/image/web_pages_structure.png)

同时网页的树结构一个父节点往往关联较多的子节点，尤其底部叶子节点数量庞大，远没有二叉树等平衡。所以网页爬虫的请求 request 直接按**层级结构**管理。再者，要爬取一个网站，有时需要爬取其几个模块，那么**采集任务的集中管理**也值得研究。基于以上两点，设计出层级结构采集框架**layer_crawler**。

### 功能说明
将从单个 request 的采集作业过程，引擎驱动，及多个采集任务的集中管理三方面来说明

#### 单个 request 的采集作业过程
通过学习 Scrapy，layer_crawler 也采用 Scheduler, Downloader, Spider, Item_Pipeline, Engine 六部分实现，不过功能上和 Scrapy 差异很大。下面一一说明。

![image](https://github.com/HectorTsang/layer_crawler/blob/master/image/module_structure.png)

上图为一个采集作业协程执行过程：
* ①引擎从 scheduler 中取出一个待采集 req
* 引擎为此 req 生成一个采集作业协程，并将此协程交给异步 asyncio 框架排期，此为并发点
* ②作业协程将此 req 交给 downloader，下载页面资源
* 作业协程 yield
* ③作业协程待下载出结果，若下载失败，将 req 重新放入 scheduler 此协程流程耗尽; 若下载成功，获得网页资源 source
* ④作业协程调用 spider，解析该 source
* ⑤作业协程获得需要入库的 item, 及需要继续采集的 req
* ⑥作业协程将 item 交给 pipeline 入库
* ⑦作业协程将 req 交给 scheduler
* 作业协程耗尽

##### scheduler 层级结构 req 调度器
layer_crawler 的层级结构通过两方面实现，其一为 req 的分层存储结构 scheduler，其二为 req 格式本身

![image](https://github.com/HectorTsang/layer_crawler/blob/master/image/scheduler.png)

scheduler 维系着两组分层集合，分别为待采集 req 集合（unvisited_reqs），用 FIFO 队列实现；正在采集的 req 集合（visiting_reqs），用 set 实现。待采集集合、正在采集集合，各深度层严格对应。

引擎从 scheduler 中取待采集 req 时，按照深度优先的策略取，优先从最深的 unvisited_reqs 中的队列中获取，逐级向上，如果依次都没有获取到 req，不等待。当从某个深度层获取到一个 req 时，req 从队列中出来，存储到对应深度的正在采集集合，这可以理解为在 visiting_reqs 中打标记的过程。随着采集作业协程的执行，如果顺利完成，则从 visiting_reqs 中擦除该标记，至此，该 req 不再存储到 scheduler 中。如果中间发生异常，则先将该 req 重新放入 unvisited_reqs 中，也要从 visiting_reqs 中擦除该标记。

![image](https://github.com/HectorTsang/layer_crawler/blob/master/image/deep_layer_reference.png)

如图所示，req 的格式采用元组表示
* layer 表示该 req 在一个采集任务中的网页层级
* method 表示下载的方法，此为 aiohttp 中的下载方法，分'GET', 'POST', 'JSON'等
* url
* referer 本次请求的的 http 协议报文 Referer 字段值
* payload 当请求需要提供内容数据时，按 tuple(tuple(key,value),...) 格式，因为 scheduler 中正在采集 set 只支持可哈希元素

scheduler 中采用深度 deep 来映射采集任务 中的网页层级 layer。因为针对同一个网站，可能存在多个模块采集任务，那么各个模块采集任务的网页层级不同。要是 scheduler 支持多任务，需要将 scheduler 的层级结构与网页层级结构解耦。scheduler 可根据不同采集任务的配置信息来个性化生成。例如针对模块 A, scheduler 维持一个深度为 2 的分层；针对模块 B, 维持一个深度 4 的分层；针对模块 B 中的第 5 层级，可能只需要增量采集，那么将此层级单独拿出来，scheduler 只需要维持一个深度为 1 的分层。

##### downloader 下载器
目前爬虫的性能瓶颈在网页下载阶段，基于 asyncio 的 aiohttp 实现异步并发网页下载。 downloader 维护着一个 clientsession 池，每个作业协程下载时，先等待从池子里取出一个空闲 clientsession，用 session 进行异步下载，返回下载结果，并交还正在使用的 session 给池子。

##### spider 解析器
对于每个业务网页层的 req，需要提取的数据一致。同时解析器都是纯粹的工具，只需要解析函数即可。不同的页面解析，都可以放在 spider module 中，建议利用**类**将不同页面的解析函数封装在一起。layer_crawler 本身不提供 spider 解析器。

##### item-pipeline 数据入库
item 是数据库存储表的表名及字段映射。layer_crawler 提供的 pipeline 只维护基础的数据库连接及游标，保证访问数据库的可行，具体的访问 ItemPipeline 需要继承 PipelineDirector，并添加具体访问方法。

#### 引擎驱动
上述单个采集作业的过程可见，需要一个引擎来驱动并发作业。下图所示为 layer_crawler 引擎的实现思路

![image](https://github.com/HectorTsang/layer_crawler/blob/master/image/engine.png)

引擎开启后，驱动一个**采集活动**循环，-- 采集活动 -- 活动结束后的操作 -- 活动间隔 -- 采集活动 --。
对于每次采集活动，分为活动中，活动收尾清场（释放 downloader 的 clientsession 资源，pipeline 的 connection 资源）。一次活动就是从第一页直到最后一页的采集全过程。
采集活动中，首先需要为 downloader 初始化 clientsession 池; 为 pipeline 初始化 connection; 为 scheduler 初始化 unvisited_reqs, 从第一页开始采集，还是由上次活动暂停时保存的 req 快照初始化。其次，需要根据执行需求选择是否在数据库中新建采集表。之后引擎就是从 scheduler 中获取 req，创建采集作业协程，并将此协程交给 asyncio 排期，开始异步并发采集。直到 scheduler 中 unvisited_reqs/visiting_reqs 都为空时，本此采集活动正常结束。

layer_crawler 提供引擎的基础套件，对于 采集活动结束后的操作、采集作业协程函数、新建采集表操作，这三个方法需要针对具体采集任务具体实现。所以需要继承基础引擎，并实现上述三个方法。

##### 采集作业协程函数
引擎从 scheduler 中获取 req 时，是按深度优先策略获取的，那么也就意味着获取的 req 是多个层级的。那么在一个采集作业协程中是怎样对各自 req 下载页面进行数据提取及入库的呢？

答案就在我们的 req 格式设计中，req 首位为 layer，对一种采集任务，每一个页面层级都有各自的解析方法，采集作业协程函数就是利用 layer 来判断如何处理。

#### 多个采集任务的集中管理
试想一下，我们利用 layer_crawler 实现一个网站某个模块的层级采集需要做什么，继承基础引擎，添加采集活动结束后的操作、采集作业协程函数、新建采集表操作三个方法，利用该任务模块的配置信息，实例化该业务引擎。该引擎维护着 downloaderDirector, 具体网站的 itemPipelineDirector, 映射任务层级的 schedulerDirector。同时在 spider 中添加该业务不同业务的数据提取方法，添加对应的 item，以及添加对应的具体入库方法。

那要添加一个任务怎么办？同样，利用基础引擎派生一个业务引擎，为该业务添加配置信息方便定制 scheduler，在 spider 中添加该业务不同业务的数据提取方法，添加对应业务各 item，新建或在已有 itemPipelineDirector 中添加具体入库方法。

如何选择启动哪个业务引擎？建议的方式为：根据配置信息，先枚举出管理的所有的采集业务，再手动选择启动的业务引擎。

### 使用指南
示例见本目录 [config.xml 配置文件](https://github.com/HectorTsang/layer_crawler/blob/master/config.xml), [crawler.py 程序入口](https://github.com/HectorTsang/layer_crawler/blob/master/crawler.py), [spider 解析器](https://github.com/HectorTsang/layer_crawler/blob/master/spider.py), [item_pipeline.py](https://github.com/HectorTsang/layer_crawler/blob/master/item_pipeline.py) 之item\itemPipelineDirector



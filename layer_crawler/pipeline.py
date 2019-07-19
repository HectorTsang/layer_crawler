# -*- coding: utf-8 -*-

"""数据库入库操作的阻塞问题：
   爬虫的效率瓶颈在页面获取等待上，依据木桶效应，只要将页面获取的总体速度提上来了，那么整个爬虫的效率会得到质的提升；
   其次，数据库连接访问，是内部访问，从采集入库上来说，可以乐观的认为是没有阀值的。
"""

import mysql.connector as mysqldb
from lxml import etree

DBKEY = {}
with open('config.xml', 'rb') as fp:
    el_config = etree.XML(fp.read())
    el_dbkey = el_config.xpath('//pipeline//dbkey[@used="yes"]')[0]
    el_host = el_dbkey.xpath('./host')[0]
    DBKEY[el_host.tag] = el_host.xpath('./text()')[0]
    el_port = el_dbkey.xpath('./port')[0]
    DBKEY[el_port.tag] = int(el_port.xpath('./text()')[0])
    el_user = el_dbkey.xpath('./user')[0]
    DBKEY[el_user.tag] = el_user.xpath('./text()')[0]
    el_password = el_dbkey.xpath('./password')[0]
    DBKEY[el_password.tag] = el_password.xpath('./text()')[0]
    el_database = el_dbkey.xpath('./database')[0]
    DBKEY[el_database.tag] = el_database.xpath('./text()')[0]
del el_config,el_dbkey,el_host,el_port,el_user,el_password,el_database, fp


class PipelineError(Exception):
    """数据库访问出现任何异常，都统一到抛出此异常"""
    def __init__(self, err):
        self.err = err

class PipelineDirector():
    """数据库访问管理器，内置建立连接并创建游标，不同类型item的入库操作"""
    def __init__(self):
        self.dbkey = DBKEY
        self.cnx = None
        self.cursor = None

    def build_cnx_cursor(self):
        """在 cnx 为空，或者 cnx 连接无效的情况下，新建/重建连接，重新创建与之关联的 cursor
        cursor 是 cnx 的关联游标，随同 cnx 创建一起创建，不再单独做判断创建

        创建时机：在程序一开始就创建一次；后面如果发生异常，再检查创建一次。因为 cnx.is_connected() 需要用到 ping() ，每次数据库操作前都去校验一次的话做太多无用功"""
        if not self.cnx:
            self.cnx = mysqldb.connect(**self.dbkey)
            self.cursor = self.cnx.cursor()
            
            # 本爬虫全部为insert语句，即使是insert一个销控表，
            # 用executemany(insert_operation, item_list), mysql.connector的处理方式为多行语法成批insert，
            # eg. INSERT INTO employees (first_name, hire_date) VALUES ('Jane', '2005-02-12'), ('Joe', '2006-05-23'), ('John', '2010-10-03');
            # 所以将事务提交模式设置为 自动提交
            self.cursor.execute('set @@autocommit=1')
            self.cnx.commit()
            # self.cursor.execute('select @@autocommit;')
            # row = self.cursor.fetchone()
            # print(row)
        elif not self.cnx.is_connected():
            self.cnx.reconnect()
            self.cursor = self.cnx.cursor()
            self.cursor.execute('set @@autocommit=1')
            self.cnx.commit()
        
    def close_cnx_cursor(self):
        # cnx,cursor .close()只是将连接和游标关闭。相当于只是切换了cnx,cursor的状态，但是cnx,cursor两个Python对象依旧存在
        # 关闭 cnx 后，cnx 可通过 is_connected() 判断连接是否有效
        # 关闭 cursor 后，重点在剪断 该 cursor 与 cnx 的关联关系
        if self.cursor:
            self.cursor.close()
        if self.cnx:
            self.cnx.close()
    
    def select(self, syntax):
        """普通sql查询"""
        try:
            self.cursor.execute(syntax)
            if self.cursor.with_rows:
                return self.cursor.fetchall()
            else:
                return self.cursor.rowcount
        except mysqldb.Error as err:
            self.build_cnx_cursor()
            raise PipelineError(err)
    
    def call_proc(self, proc_pi, args=()):
        """执行存储过程"""
        try:
            self.cursor.callproc(proc_pi, args)
            if self.cursor.with_rows:
                return self.cursor.fetchall()
            else:
                return self.cursor.rowcount
        except mysqldb.Error as err:
            self.build_cnx_cursor()
            raise PipelineError(err)
    
    def insert_many(self, syntax, items):
        """入库一组数据"""
        try:
            self.cursor.executemany(syntax, items)
        except mysqldb.Error as err:
            self.build_cnx_cursor()
            raise PipelineError(err)
    
    def insert_one(self, syntax, item):
        """入库一条数据"""
        try:
            self.cursor.execute(syntax, item)
        except mysqldb.Error as err:
            self.build_cnx_cursor()
            raise PipelineError(err)

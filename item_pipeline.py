# -*- coding: utf-8 -*-

TABLE_NAME_PREFIX = 'cn_gov_sz_zjj'
from layer_crawler.pipeline import PipelineDirector, PipelineError

class MacroRoomUsegroup():
    table = TABLE_NAME_PREFIX+'_marco_room_usegroup'
    statics_date = 'statics_date'
    cityarea = 'cityarea'
    usetype = 'usetype'
    sale_count = 'sale_count'
    sale_area = 'sale_area'
    on_sale_count = 'on_sale_count'
    on_sale_area = 'on_sale_area'

class MacroResidenceAreagroup():
    table = TABLE_NAME_PREFIX+'_marco_residence_areagroup'
    statics_date = 'statics_date'
    cityarea = 'cityarea'
    areagroup = 'areagroup'
    sale_count = 'sale_count'
    sale_area = 'sale_area'

class Batch():
    table = TABLE_NAME_PREFIX+'_batch'
    req0_page = 'req0_page'
    batch_info_url = 'batch_info_url'
    base_info_url = 'base_info_url'
    idx = 'idx'
    batch_name = 'batch_name'
    base_name = 'base_name'
    developer = 'developer'
    cityarea = 'cityarea'
    allow_sell_date = 'allow_sell_date'
    req1_batch = 'req1_batch'
    req1_base = 'req1_base'

class Base():
    table = TABLE_NAME_PREFIX+'_base'
    req1_base = 'req1_base'
    base_name = 'base_name'
    land_number = 'land_number'
    address = 'address'
    land_recv_date = 'land_recv_date'
    land_cityarea = 'land_cityarea'
    land_ownertype = 'land_ownertype'
    land_approval_org = 'land_approval_org'
    land_pact_number = 'land_pact_number'
    land_use_age_limit = 'land_use_age_limit'
    land_supplemental_agreement = 'land_supplemental_agreement'
    land_plan = 'land_plan'
    house_usetype = 'house_usetype'
    land_usetype = 'land_usetype'
    land_degree = 'land_degree'
    total_area = 'total_area'
    use_area = 'use_area'
    building_area = 'building_area'
    presell_count = 'presell_count'
    presell_area = 'presell_area'
    ensell_count = 'ensell_count'
    ensell_area = 'ensell_area'
    tel1 = 'tel1'
    tel2 = 'tel2'

class Building():
    table = TABLE_NAME_PREFIX+'_building'
    req1_base = 'req1_base'
    base_name = 'base_name'
    building_name = 'building_name'
    project_plan_certificate = 'project_plan_certificate'
    project_execute_certificate = 'project_execute_certificate'
    building_url = 'building_url'
    req2_building = 'req2_building'

class Branch():
    table = TABLE_NAME_PREFIX+'_branch'
    req2_building = 'req2_building'
    branch_url = 'branch_url'
    branch_name = 'branch_name'
    req3_branch = 'req3_branch'

class RoomState():
    table = TABLE_NAME_PREFIX+'_room_state'
    req3_branch = 'req3_branch'
    flr = 'flr'
    room_number = 'room_number'
    room_url = 'room_url'
    room_state = 'room_state'
    req4_roomattri = 'req4_roomattri'

class RoomAttri():
    table = TABLE_NAME_PREFIX+'_room_attri'
    req4_roomattri = 'req4_roomattri'
    base_name = 'base_name'
    branch_name = 'branch_name'
    pact_number = 'pact_number'
    public_price = 'public_price'
    flr = 'flr'
    room_number = 'room_number'
    usetype = 'usetype'
    presell_building_area = 'presell_building_area'
    presell_inner_area = 'presell_inner_area'
    presell_other_area = 'presell_other_area'
    completed_inner_area = 'completed_inner_area'
    completed_other_area = 'completed_other_area'
    completed_building_area = 'completed_building_area'

class ItemPipelineDirector(PipelineDirector):
    """数据库访问管理器，内置建立连接并创建游标，不同类型item的入库操作"""
    def __init__(self):
        PipelineDirector.__init__(self)

        self.sql_syntax_insert_MacroRoomUsegroup = 'insert into {0} ({1},{2},{3},{4},{5},{6},{7}) values (%s,%s,%s,%s,%s,%s,%s)'.format(MacroRoomUsegroup.table, MacroRoomUsegroup.statics_date, MacroRoomUsegroup.cityarea, MacroRoomUsegroup.usetype, MacroRoomUsegroup.sale_count, MacroRoomUsegroup.sale_area, MacroRoomUsegroup.on_sale_count, MacroRoomUsegroup.on_sale_area)
        self.sql_syntax_insert_MacroResidenceAreagroup = 'insert into {0} ({1},{2},{3},{4},{5}) values (%s,%s,%s,%s,%s)'.format(MacroResidenceAreagroup.table, MacroResidenceAreagroup.statics_date, MacroResidenceAreagroup.cityarea, MacroResidenceAreagroup.areagroup, MacroResidenceAreagroup.sale_count, MacroResidenceAreagroup.sale_area)
        self.sql_syntax_insert_Batch = 'insert into {0} ({1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(Batch.table, Batch.req0_page, Batch.batch_info_url, Batch.base_info_url, Batch.idx, Batch.batch_name, Batch.base_name, Batch.developer, Batch.cityarea, Batch.allow_sell_date, Batch.req1_batch, Batch.req1_base)
        self.sql_syntax_insert_Base = 'insert into {0} ({1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(Base.table, Base.req1_base, Base.base_name, Base.land_number, Base.address, Base.land_recv_date, Base.land_cityarea, Base.land_ownertype, Base.land_approval_org, Base.land_pact_number, Base.land_use_age_limit, Base.land_supplemental_agreement, Base.land_plan, Base.house_usetype, Base.land_usetype, Base.land_degree, Base.total_area, Base.use_area, Base.building_area, Base.presell_count, Base.presell_area, Base.ensell_count, Base.ensell_area, Base.tel1, Base.tel2)
        self.sql_syntax_insert_Building = 'insert into {0} ({1},{2},{3},{4},{5},{6},{7}) values (%s,%s,%s,%s,%s,%s,%s)'.format(Building.table, Building.req1_base, Building.base_name, Building.building_name, Building.project_plan_certificate, Building.project_execute_certificate, Building.building_url, Building.req2_building)
        self.sql_syntax_insert_Branch = 'insert into {0} ({1},{2},{3},{4}) values (%s,%s,%s,%s)'.format(Branch.table, Branch.req2_building, Branch.branch_url, Branch.branch_name, Branch.req3_branch)
        self.sql_syntax_insert_RoomState = 'insert into {0} ({1},{2},{3},{4},{5},{6}) values (%s,%s,%s,%s,%s,%s)'.format(RoomState.table, RoomState.req3_branch, RoomState.flr, RoomState.room_number, RoomState.room_url, RoomState.room_state, RoomState.req4_roomattri)
        self.sql_syntax_insert_RoomAttri = 'insert into {0} ({1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(RoomAttri.table, RoomAttri.req4_roomattri, RoomAttri.base_name, RoomAttri.branch_name, RoomAttri.pact_number, RoomAttri.public_price, RoomAttri.flr, RoomAttri.room_number, RoomAttri.usetype, RoomAttri.presell_building_area, RoomAttri.presell_inner_area, RoomAttri.presell_other_area, RoomAttri.completed_inner_area, RoomAttri.completed_other_area, RoomAttri.completed_building_area)
    
    def insert_macro_room_usegroup_items(self, macro_room_usegroup_items):
        """宏观统计-按用途分组入库"""
        self.insert_many(self.sql_syntax_insert_MacroRoomUsegroup, macro_room_usegroup_items)
    
    def insert_macro_residence_areagroup_items(self, macro_residence_areagroup_items):
        """宏观统计-住宅按面积分组入库"""
        self.insert_many(self.sql_syntax_insert_MacroResidenceAreagroup, macro_residence_areagroup_items)
    
    def insert_batch_items(self, batch_items):
        """预售证入库"""
        self.insert_many(self.sql_syntax_insert_Batch, batch_items)
    
    def insert_base_item(self, base_item):
        """项目入库"""
        self.insert_one(self.sql_syntax_insert_Base, base_item)
    
    def insert_building_items(self, building_items):
        """楼栋入库"""
        self.insert_many(self.sql_syntax_insert_Building, building_items)
    
    def insert_branch_items(self, branch_items):
        """楼栋下座号入库"""
        self.insert_many(self.sql_syntax_insert_Branch, branch_items)
    
    def insert_roomstate_items(self, roomstate_items):
        """房间状态入库"""
        self.insert_many(self.sql_syntax_insert_RoomState, roomstate_items)
    
    def insert_roomattri_item(self, roomattri_item):
        """房间属性入库"""
        self.insert_one(self.sql_syntax_insert_RoomAttri, roomattri_item)

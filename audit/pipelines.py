# -*- coding: utf-8 -*-
import pymongo
from audit.items import NwperItem, CpaItem, ProfileItem, FirmItem

class AuditPipeline(object):
    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://localhost:27017')
        self.database = self.client['Auditors']
        self.cpa_info = self.database['cpa_info']
        self.nwp_info = self.database['nwp_info']
        self.cpa_profile = self.database['cpa_profile']
        self.audit_firm = self.database['audit_firm']


    def process_item(self, item, spider):
        if isinstance(item, NwperItem):
            self.nwp_info.insert_one(item)

        if isinstance(item, CpaItem):
            self.cpa_info.insert_one(item)


        if isinstance(item, FirmItem):
            self.audit_firm.insert_one(item)


        if isinstance(item, ProfileItem):
            self.cpa_profile.insert_one(item)

    def close_spider(self, spider):
        self.client.close()


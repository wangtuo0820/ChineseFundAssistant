import pymongo
import time
from datetime import datetime
from datetime import timedelta
from dateutil import parser



# 当需要对一个字段进行排序时，需要将其设为索引，额外建一个表
# self.fund_info.create_index("found_date") 

class Selector():
    def __init__(self, fund_info=None):
        self.fund_info = fund_info
        self.function_class = {
                'found_date': self.query_by_found_date,
                'found_length': self.query_by_found_length,
                'fund_scale': self.query_by_fund_scale,
                'grow_rate': self.query_by_grow_rate,
                'mdd': self.query_by_mdd,
                'grow_rate2': self.query_by_grow_rate,
                'mdd2': self.query_by_mdd,
                'grow_rate3': self.query_by_grow_rate,
                'mdd3': self.query_by_mdd,
        }

    def select_by_multi(self, **kwargs):
        multi_query = {}
        print('-'*30)
        for k, v in kwargs.items():
            query = self.function_class[k](*v)
            multi_query.update(query)
        print('-'*30)
        if self.fund_info == None:
            raise TypeError('Please initialize Selector with fund_info collection!')
        myres = self.fund_info.find(multi_query)
        return myres

    def find_by_query(self, query):
        if self.fund_info == None:
            raise TypeError('Please initialize Selector with fund_info collection!')
        myres = self.fund_info.find(query)
        return myres


    def query_by_found_length(self, last=10, how='$gt'):
        last = timedelta(days=last)
        start = datetime.now() - last
        
        print("查找成立时间{}{}的基金".format("晚于" if how=='$gt' else "早于", start.strftime('%Y年%m月%d日')))
    
        myquery = {
                "found_date":
                {
                    how: start
                }
        }
        # myres = self.fund_info.find(myquery)
        return myquery
    
    def query_by_found_date(self, before='2020-11-1', how='$gt'):
        start = before
        start = datetime.strptime(start, "%Y-%m-%d")
    
        print("查找成立时间{}{}的基金".format("晚于" if how=='$gt' else "早于", start.strftime('%Y年%m月%d日')))
    
        myquery = {
                "found_date":
                {
                    how: start
                }
        }
        # myres = self.fund_info.find(myquery)
        return myquery
    
    def query_by_fund_scale(self, min_scale, max_scale):
        print("查找基金规模{}亿元到{}亿元之间的基金".format(min_scale, max_scale))
        myquery = {
                "fund_scale":
                {
                    '$gt': min_scale,
                    '$lt': max_scale
                }
        }
        # myres = self.fund_info.find(myquery)
        return myquery
    
    def query_by_grow_rate(self, min_grow_rate, max_grow_rate, year=None):
        print("查找增长率{:.2%}到{:.2%}之间的基金".format(min_grow_rate, max_grow_rate))
        myquery = {
                "grow_rate" if year is None else f'grow_rate@{year}':
                {
                    '$gt': min_grow_rate,
                    '$lt': max_grow_rate
                }
        }
        # myres = self.fund_info.find(myquery)
        return myquery
    
    def query_by_mdd(self, min_mdd, max_mdd, year=None):
        min_mdd, max_mdd = -max_mdd, -min_mdd
        print("查找最大回撤{:.2%}到{:.2%}之间的基金".format(max_mdd, min_mdd))
        myquery = {
                "mdd" if year is None else f'mdd@{year}':
                {
                    '$gt': min_mdd,
                    '$lt': max_mdd
                }
        }
        # myres = self.fund_info.find(myquery)
        return myquery

if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['funddb']
    fund_info = mydb['fund_info']

    selector = Selector(fund_info)

    myquery = selector.query_by_found_date(before='2020-11-1')
    myres = selector.find_by_query(myquery)
    for item in myres:
        print(item['name'])

    myres = selector.select_by_multi(mdd=[0.1,0.11], grow_rate=[0.2,0.5])
    for item in myres:
        print(item['name'])

    myres = selector.select_by_multi(found_length=[10])
    for item in myres:
        print(item['name'])

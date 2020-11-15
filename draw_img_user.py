import pymongo
import matplotlib.pyplot as plt

from Selector import Selector
import numpy as np

from create_mdd_growrate import create_mdd_growrate_by_user

if __name__ == "__main__":
    start = '2019-1-1'
    end = '2020-1-1'
    query_id = start+'-'+end
    query_id = create_mdd_growrate_by_user(start, end )

    start2 = '2017-1-1'
    end2 = '2020-11-11'
    query_id2 = start2+'-'+end2
    query_id2 = create_mdd_growrate_by_user(start2, end2)

    #start3 = '2019-1-1'
    #end3 = '2020-11-11'
    #query_id3 = create_mdd_growrate_by_user(start3, end3)
    query_id3 = 2020

    myclient = pymongo.MongoClient('mongodb://localhost:27017/')
    mydb = myclient['funddb']
    fund_info = mydb['fund_info']
    fund_trend = mydb['fund_trend']

    selector = Selector(fund_info)
    
    myres = selector.select_by_multi(
            mdd=[0.02, 0.15, query_id], grow_rate=[0.2, 0.8, query_id], 
            mdd2=[0.02, 0.2, query_id2], grow_rate2=[1, 2.0, query_id2], 
            mdd3=[0.02, 0.15, query_id3], grow_rate3=[0.2, 0.8, query_id3], 
            fund_scale=[1,1000])

    myres = list(myres)

    for q in [query_id, query_id2, query_id3]:
        plt.figure()
        x, y, s, n = [], [], [], []
        cnt = 0
        for item in myres:
            cnt += 1
            name = item['name']
            mdd = item[f'mdd@{q}']
            grow_rate = item[f'grow_rate@{q}']
            fund_scale = (item['fund_scale']*100)**0.8

            x.append(mdd)
            y.append(grow_rate)
            s.append(fund_scale)
            plt.text(mdd,grow_rate, name, fontsize=13)

        print(f"总共发现{cnt}只符合条件的基金")
        plt.scatter(x,y, c=list(range(len(x))), s=s)
        plt.xlabel("最大回撤")
        plt.ylabel("增长率")
        plt.title(q)

    plt.show()

    # for q in [query_id, query_id2, query_id3]:
    #     fund_info.update_one(
    #             {}, 
    #             {
    #                 '$unset': 
    #                 {
    #                     f'mdd@{q}': "",
    #                     f'grow_rate@{q}': ""
    #                 }
    #             }, False, True,
    #     )

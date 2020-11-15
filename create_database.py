# -*- coding: utf-8 -*-

import requests, re, os
from  multiprocessing import Pool

from Parser import MyParser
from get_worth import plot_trend
from FakeUAGetter import my_fake_ua
from tqdm import tqdm

import pymongo

fail_funds= []

def get_find_list_generator():

    header = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                    'Chrome/78.0.3904.108 Safari/537.36'}
    page = requests.get('http://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=1&lx=1&letter=&gsid=&text=&sort=zdf,'
                                'desc&page=1,9999&feature=|&dt=1536654761529&atfc=&onlySale=0', headers=header)
    # 基金目录
    fund_list = re.findall(r'"[0-9]{6}",".+?"', page.text)
    sum_of_fund = len(fund_list)
    print('共发现' + str(sum_of_fund) + '个基金')

    fund_list_generator = (f'{i[1:7]},{i[10:-1]}' for i in fund_list)
    return fund_list_generator

def work(item):

    code, name, idx = item.split(',')
    print(idx)
    fund_url = 'http://fund.eastmoney.com/' + code + '.html'

    page_text = parse_page(fund_url)
    parser = MyParser(page_text)
    fund_type, fund_performance = parser.parse_all()
    parser.parse_netvalue()

    try:
        ACWorthTrend, grow_rate, mdd = plot_trend(code)
        fund_info = {'_id': code, 'name': name, 
                'grow_rate': grow_rate,
                'mdd': mdd}
        fund_trend = {'_id':code, 
                    'acworth_trend': ACWorthTrend}
        mycol_info.insert_one(fund_info)
        mycol_trend.insert_one(fund_trend)

    except:
        fail_fund_info = ','.join([name, code, fund_type]) + '\n'
        fail_funds.append(fail_fund_info)
        print(fail_fund_info)


def parse_page(url, timeout=3):
    header = {"User-Agent": my_fake_ua.random}
    page = requests.get(url, headers=header, timeout=timeout)
    page.encoding = 'utf-8'
    return page.text

if __name__ == '__main__':
    fund_list_generator = get_find_list_generator()
    fund_info_list = []

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['funddb']
    mycol_info = mydb['fund_info']
    mycol_info.delete_many({})
    mycol_trend = mydb['fund_trend']
    mycol_trend.delete_many({})



    # for i, item in enumerate(tqdm(fund_list_generator)):
    param = []
    for i, item in enumerate(fund_list_generator):
        param.append(item+f',{i}')

    p= Pool(8)
    p.map(work, param)
    p.close()
    p.join()

    with open('fail_funds.txt', 'w') as f:
        f.writelines(fail_funds)

    print('--'*20)
    print(fail_funds)

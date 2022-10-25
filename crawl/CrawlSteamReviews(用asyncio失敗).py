#%%
import requests
import json
import pandas as pd
import numpy as np
games = pd.read_csv('games.csv',usecols=['AppID','Name'])
summary = pd.read_csv('review_summary_asyncio.csv',index_col=0)
df = pd.merge(games, summary, on='AppID')
THR = 10
df = df[(df['total_review_num']>=THR)].reset_index(drop=True)
# appids = df.AppID.tolist()
# 先跑評論數最多的，有利於asyncio的效率提升
appids = df.sort_values(by='total_review_num',ascending=0)['AppID'].tolist()
# appids_temp = [730]
#%%
%%time
import urllib
import math
import asyncio
import aiohttp
import nest_asyncio
import paco # 用來限制asyncio的concurrency數量，避免被ban(sleep沒用)
nest_asyncio.apply() #解決async噴錯的問題

# ITERATION = 50 # 50次約14秒
NUM_PER_PAGE = 100

# https://partner.steamgames.com/doc/store/getreviews
async def do_requests(appid, session):
    try:
        num = summary[summary['AppID']==appid]['total_review_num'].values[0]
        # print ('共{}篇評論'.format(num))
        for i in range(math.ceil(num/NUM_PER_PAGE)):
        # for i in range(5):
            if i == 0:
                queryString = {
                                'json' : 1,
                                'cursor' : cursor_dict[appid], # 不用特別傳。加了反而導致每次return的cursor都一樣，不知為啥
                                'language': 'english',
                                'filter' : 'updated',
                                #    'purchase_type':'steam', # 由在 Steam 上自行購買產品的使用者撰寫（預設）
                                'num_per_page':str(NUM_PER_PAGE) # 預設20，上限100。不知為啥設100會變成94篇，所以先設99
                                }
            # print (i, '1 ',appid, cursor_dict[appid])
            query_code = urllib.parse.urlencode(queryString)
            url="https://store.steampowered.com/appreviews/"+str(appid)+"?" +query_code
            
            async with session.get(url) as response: # 問題在這邊的url不會變
                data = await response.json()
                # print (i, cursor_dict[appid], data['cursor'],url)
                if (data["cursor"] == cursor_dict[appid]) & (data["cursor"]!='*'):
                    # print (i, '這裡沒東西了= =')
                    break # 代表再翻頁也沒東西了
                for item in data["reviews"]:
                    try: # 確保每個值都有成功，再append
                        s, r, v, w, vu = item["author"]["steamid"], item["review"], item["voted_up"], item["weighted_vote_score"], item["votes_up"]
                    except:
                        print (appid, ' data error.')
                    appid_list.append(appid)
                    userid_list.append(s)
                    review_list.append(r)
                    like_list.append(v)
                    refvalue_list.append(w)
                    voteup_list.append(vu)
                if (data["cursor"] == cursor_dict[appid]) or (data['query_summary']['num_reviews']<NUM_PER_PAGE):
                    print (appid,'沒東西了= =')
                    break # 代表再翻頁也沒東西了
                else:
                    # cursor_list.append(data["cursor"])
                    # queryString["cursor"] = data["cursor"]
                    cursor_dict[appid] = data["cursor"]
                    # print (i, '2 ',appid, data["cursor"])
    except Exception as e:
        print (appid, e, ' error.')



async def main(appids_temp):
    async with aiohttp.ClientSession() as session:
        await paco.gather(*[do_requests(appid, session) for appid in appids_temp], #用paco來限制爬蟲速度，否則會被ban。不用paco的話就是用上面那行
        limit = 10 # max concurrency limit. Use 0 for no limit.
        )
#%%
%%time
# 跑完appids[:20]從14秒加快到2.6秒
SLICE = 9
# for a in range(SLICE): # 4hr47分
for a in [0]:
    appid_list, userid_list, review_list, like_list, refvalue_list, voteup_list  = [], [], [], [], [], []
    # appids_sliced = appids[a::SLICE]
    appids_sliced = [201810, 2054100] #, 
    cursor_dict = {a:'*' for a in appids_sliced}
    print ('第{}批開始...'.format(a))
    asyncio.run(main(appids_sliced))
    
    print ('Review總數：', df[df.AppID.isin(appids_sliced)]['total_review_num'].sum())
    print ('已成功爬取：',len(like_list))

    reviews_df = pd.DataFrame()
    reviews_df["AppID"] = appid_list
    reviews_df["UserID"] = userid_list
    reviews_df["Like"] = like_list #是否喜歡
    reviews_df["RefValue"] = refvalue_list #評論的參考價值分數
    reviews_df["VoteUp"] = voteup_list #認為此篇評論值得參考的使用者人數
    reviews_df["Review"] = review_list
    reviews_df.to_pickle('reviews/ReviewsDF_99.pkl'.format(a))
    print ('重複',reviews_df[reviews_df.duplicated(subset=['AppID','UserID'])].shape[0])

#%%
d = pd.read_pickle('reviews/ReviewsDF_0.pkl')
print (d.shape)
print (d[d.duplicated(subset=['AppID','UserID'])].shape)

#%%
import urllib.request as req
appid = 730
# cursor_dict[appid] = '*'
queryString = {
            'json' : 1,
            'cursor' : cursor_dict[appid], # 不用特別傳。加了反而導致每次return的cursor都一樣，不知為啥
            'language': 'english',
            #    'purchase_type':'all', # 由在 Steam 上自行購買產品的使用者撰寫（預設）
            'num_per_page': 100# 預設20，上限100。不知為啥設100會變成94篇，所以先設99
            }

review_list = []
# for i in range(5):
query_code = urllib.parse.urlencode(queryString)
url="https://store.steampowered.com/appreviews/"+str(appid)+"?" +query_code
            
with req.urlopen(url+query_code) as response:
    data = json.load(response)
# print (data['query_summary']['total_reviews'])
# print (data['query_summary']['num_reviews'])
for i in range(100):
    print (i)
    data['reviews'][i]['review']
    pass
# cursor_dict[appid] = data['cursor']
# %%


# 測試

appid_list, userid_list, review_list, like_list, refvalue_list, voteup_list  = [], [], [], [], [], []
cursor_list = []
# appids_sliced = appids[5000:5001]
appids_sliced = [730]
cursor_dict = {a:'*' for a in appids_sliced}
asyncio.run(main(appids_sliced))

print ('Review總數：', df[df.AppID.isin(appids_sliced)]['total_review_num'].sum())
print ('已成功爬取：',len(like_list))

d = pd.DataFrame()
d["AppID"] = appid_list
d["UserID"] = userid_list
d["Like"] = like_list #是否喜歡
d["RefValue"] = refvalue_list #評論的參考價值分數
d["VoteUp"] = voteup_list #認為此篇評論值得參考的使用者人數
d["Review"] = review_list
d.to_pickle('reviews/ReviewsDF_99.pkl')

print (d.shape)
print (d[d.duplicated(subset=['AppID','UserID'])].shape)
#%%
queryString = {
            'json' : 1,
            'filter' : 'recent',
            'cursor' : '*',
            'cursor' : 'AoJ40PSE24IDc9GQzQM=', # 不用特別傳。加了反而導致每次return的cursor都一樣，不知為啥
            # 'language': 'english',
            #    'purchase_type':'steam', # 由在 Steam 上自行購買產品的使用者撰寫（預設）
            'num_per_page':100 # 預設20，上限100。不知為啥設100會變成94篇，所以先設99
            }
query_code = urllib.parse.urlencode(queryString)
url="https://store.steampowered.com/appreviews/"+str(201810)+"?" +query_code
r = requests.get(url, params = queryString)
print (r.json()['cursor'])
print (len(r.json()['reviews']))








#%%

async def do_requests(i, session):
    token = 0
    for i in range(10):
        # 必須要有上一次的回傳結果才能進行下一次的request
        async with session.get(url+str(token)) as response: 
            data = await response.json()
            token = data['result']

async def main():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[do_requests(i, session) for i in range(100)])

asyncio.run main()
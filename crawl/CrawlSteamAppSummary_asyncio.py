#%%
import requests
import json
import pandas as pd

games = pd.read_csv('games.csv',usecols=['AppID','Genres','Categories','Windows'])
games = games.dropna(subset=['Genres'])
games = games[games['Windows']==True].drop(['Windows','Categories'],axis=1)

genres_count = games['Genres'].str.split(',').explode('Genres').value_counts()
# Genres較有參考價值
#%%
%%time
import asyncio
import aiohttp
import nest_asyncio
import paco # 用來限制asyncio的concurrency數量，避免被ban(sleep沒用)
nest_asyncio.apply() #解決async噴錯的問題

# GENRE = 'Racing'
# applist = games[games.Genres.str.contains(GENRE)]['AppID'].tolist()
applist = games.AppID.tolist()
total_reviews_list, total_pos_reviews_list, total_neg_reviews_list, appid_list = [], [], [], []

async def do_requests(appid, session):
    try:
        async with session.get("https://store.steampowered.com/appreviews/{}?json=1".format(appid), params={'language':'english','purchase_type':'all'}) as response:
            res = await response.json()
            total_reviews_list.append(res['query_summary']['total_reviews'])
            total_pos_reviews_list.append(res['query_summary']['total_positive'])
            total_neg_reviews_list.append(res['query_summary']['total_negative'])
            appid_list.append(appid)
    except Exception as e:
        print (appid, e)

async def main():
    async with aiohttp.ClientSession() as session:
        # await asyncio.gather(*[do_requests(appid, session) for appid in applist[:100]]) 
        await paco.gather(*[do_requests(appid, session) for appid in applist], #用paco來限制爬蟲速度，否則會被ban。不用paco的話就是用上面那行
        limit = 10 # max concurrency limit. Use 0 for no limit.基本上數量設多少就會是幾倍快
        )

asyncio.run(main())
#%%
df = pd.DataFrame()
df['AppID'] = appid_list
df['total_review_num'] = total_reviews_list
df['total_pos_num'] = total_pos_reviews_list
df['total_neg_num'] = total_neg_reviews_list
if 'total_review_num' not in games.columns:
    games = pd.merge(games, df, on='AppID')
games.to_csv('review_summary_purchaseall_asyncio.csv')
#%%
REVIEW_NUM_THRESHOLD = 10
print ('共{}個遊戲'.format(games.shape[0]))
for i in [1,2,3,4,5,6,7,8,9,10,15,20,30,40,50,100,500,1000]:
    REVIEW_NUM_THRESHOLD = i
    print ('評論總數低於{}共{}個'.format(REVIEW_NUM_THRESHOLD, games[games['total_review_num']<REVIEW_NUM_THRESHOLD].shape[0]))

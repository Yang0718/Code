#%%
import requests
import json
import pandas as pd
import numpy as np
games = pd.read_csv('games.csv',usecols=['AppID','Name','Screenshots'])
summary = pd.read_csv('review_summary_asyncio.csv',index_col=0)
df = pd.merge(games, summary, on='AppID')
THR = 10
df = df[(df['total_review_num']>=THR) & (~df['Screenshots'].isnull())].reset_index(drop=True)
df['Screenshots'] = df.Screenshots.str.split(",")

# 下載圖片
# import urllib.request

# https://steamcdn-a.akamaihd.net/steam/apps/730/header.jpg
# urllib.request.urlretrieve("https://cdn.akamai.steamstatic.com/steam/apps/730/ss_d196d945c6170e9cadaf67a6dea675bd5fa7a046.1920x1080.jpg?t=1641233427", "local-filename.jpeg")

#%%
%%time
# img = urllib.request.urlopen().read()
from urllib.request import urlopen
from PIL import Image
import h5py # h5py是最有效率存取大量圖片的方法
            # conda install -c anaconda h5py

# 建立h5py資料集
USENUM = 0 # 0表用全部
if USENUM:
    urls = [x for i in df['Screenshots'][:USENUM] for x in i]
    appids = [[df['AppID'][i]]*len(df["Screenshots"][i]) for i in range(USENUM)]
    appids = [a for i in appids for a in i] # flatten
else:
    urls = [x for i in df['Screenshots'] for x in i]
    appids = [[df['AppID'][i]]*len(df["Screenshots"][i]) for i in range(df.shape[0])]
    appids = [a for i in appids for a in i] # flatten

#%%
IMG_HEIGHT = 180
IMG_WIDTH = 320

if 'hf' in locals():
    if hf: # 若hf還開著
        hf.close() # 如果上一個hf還開著，就會不能重建
        print ('previous hf closed.')

hf=h5py.File('screenshots.hdf5', 'w')
hf.create_dataset("imgs",
                #   shape=(None,) + IMGSIZE,
                  shape =  (len(urls), IMG_HEIGHT, IMG_WIDTH, 3),
                #   maxshape = (None, 320, 180),
                  chunks=True,
                  compression="gzip",
                  compression_opts=4) # default=4
hf.create_dataset("appids",
            shape = (len(urls)),
            compression="gzip",
            compression_opts=4)

# url = "https://cdn.akamai.steamstatic.com/steam/apps/730/ss_d196d945c6170e9cadaf67a6dea675bd5fa7a046.1920x1080.jpg?t=1641233427"
# img_sizes = [(1920, 1080), (1600, 900), (1280, 720), (960, 600), (1728, 1080), (1831, 1030), (1364, 768), (1535, 862), (1333, 632), (1080, 540), (1758, 934), (1759, 939), (1759, 927), (1762, 941), (1360, 765), (1407, 791), (1624, 900), (710, 443), (1272, 718), (1286, 706), (1920, 1079), (1280, 960), (1440, 1080), (1295, 728), (1848, 1044), (1256, 614), (1466, 730), (1684, 947), (1908, 991), (1908, 996), (1910, 1000), (1898, 984), (1850, 941), (1916, 990), (1680, 1050), (1920, 984), (1279, 719), (1120, 1008), (1118, 1009), (960, 640), (640, 480), (800, 600), (799, 601), (797, 598), (800, 599), (1366, 768), (1919, 1080), (1920, 1050), (1918, 1080), (1593, 891), (1470, 828), (1674, 945), (1459, 835), (1024, 576), (1024, 640), (1620, 1080), (1024, 768), (1228, 1080), (1774, 997), (1915, 1079), (1919, 1079), (1910, 1046), (1918, 835), (1920, 983), (1747, 950), (1284, 967), (1888, 942), (1081, 920), (1920, 1040)]
# 基本上是16:9
# 跑100筆APP的所有截圖，2分半
#%%
# %%time
# for i, url in enumerate(urls):
#     try:
#         img = Image.open(urlopen(url)).resize((IMG_WIDTH, IMG_HEIGHT))
#         hf["imgs"][i,...] = img
#     except Exception as e:
#         try:
#             hf["imgs"][i,...] = img.convert(mode="RGB") #黑白圖片channel=1，要修正
#         except Exception as e:
#             print (i, e, url)      
# hf.close()


#%%
%%time
# ASYNCIO VERSION
import asyncio
import aiohttp
import nest_asyncio
import io
import paco # 用來限制asyncio的concurrency數量，避免被ban(sleep沒用)
nest_asyncio.apply() #解決async噴錯的問題


MAX_CONCURRENCY = 20
async def do_requests(appid, url, session):
    try:
        async with session.get(url) as response:
            res = await response.read()
            img = Image.open(io.BytesIO(res))
            try:
                img = Image.open(urlopen(url)).resize((IMG_WIDTH, IMG_HEIGHT)).convert(mode="RGB") #可能會有黑白圖片channel=1，要修正
                img_list.append(img)
                app_list.append(appid)
            except Exception as e:
                print (appid, e, url)
                error_url_list.append(url)
                error_appid_list.append(appid)
        
    except Exception as e:
        print (url, e)
        error_url_list.append(url)
        error_appid_list.append(appid)


async def main(appids,urls):
    tasks = []
    async with aiohttp.ClientSession() as session:
        # await asyncio.gather(*[do_requests(appid, session) for appid in applist[:100]]) 
        await paco.gather(*[do_requests(appid, url, session) for (appid,url) in zip(appids,urls)], #用paco來限制爬蟲速度，否則會被ban。不用paco的話就是用上面那行
        limit = MAX_CONCURRENCY  # max concurrency limit. Use 0 for no limit.基本上數量設多少就會是幾倍快
        )

# 太大了容易噴錯(不知是不是記憶體爆掉)，改成分批跑&存
SEG = 5000 #每個batch跑幾筆
for i in range(round(len(appids)/SEG)):
    start, end = i*SEG, (i+1)*SEG
    img_list, app_list = [], []
    error_url_list, error_appid_list = [], []
    try:
        asyncio.run(main(appids[start:end],urls[start:end]))
        img_np = np.empty((len(img_list), IMG_HEIGHT, IMG_WIDTH, 3))
        img_np[:,...] = [np.array(im) for im in img_list] 
        hf["appids"][start:end] = app_list
        hf["imgs"][start:end,...] = img_np
        print ('第{}至{}筆，成功'.format(start, end))
    except Exception as e:
        print ('第{}至{}筆，失敗'.format(start, end))
hf.close()
#%%
import requests
import json
import pandas as pd
import numpy as np
import utils
# games = pd.read_csv('games.csv',usecols=['AppID','Name','Header image'])
# summary = pd.read_csv('review_summary_asyncio.csv',index_col=0)
# df = pd.merge(games, summary, on='AppID')
# THR = 10
df = utils.readData(REVIEW_THRESHOLD=10)
df = df[~df['Header image'].isnull()].reset_index(drop=True)# 但header image沒有NULL
#%%
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
    appids = df['AppID'][:USENUM]
else:
    appids = df['AppID']
IMG_HEIGHT = 180
IMG_WIDTH = 320
if 'hf' in locals():
    if hf: # 若hf還開著
        hf.close() # 如果上一個hf還開著，就會不能重建
        print ('previous hf closed.')

hf=h5py.File('headers.hdf5', 'w')
hf.create_dataset("imgs",
                #   shape=(None,) + IMGSIZE,
                  shape =  (df.shape[0], IMG_HEIGHT, IMG_WIDTH, 3),
                #   maxshape = (None, 320, 180),
                  chunks=True,
                  compression="gzip",
                  compression_opts=4) # default=4，最高9。9的耗時是4的4.72倍，但大小差沒多少
hf.create_dataset("appids",
            shape = (df.shape[0]),
            compression="gzip",
            compression_opts=4)
#%%
%%time
# ASYNCIO VERSION
import asyncio
import aiohttp
import nest_asyncio
import io
import paco # 用來限制asyncio的concurrency數量，避免被ban(sleep沒用)
nest_asyncio.apply() #解決async噴錯的問題

img_list, app_list = [], []
error_url_list, error_appid_list = [], []
MAX_CONCURRENCY = 20 # 25153張圖片，27分鐘

async def do_requests(appid, session):
    url = "https://steamcdn-a.akamaihd.net/steam/apps/{}/header.jpg".format(appid)
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


async def main():
    tasks = []
    async with aiohttp.ClientSession() as session:
        # await asyncio.gather(*[do_requests(appid, session) for appid in applist[:100]]) 
        await paco.gather(*[do_requests(appid, session) for appid in appids], #用paco來限制爬蟲速度，否則會被ban。不用paco的話就是用上面那行
        limit = MAX_CONCURRENCY # max concurrency limit. Use 0 for no limit.基本上數量設多少就會是幾倍快
        )

asyncio.run(main())
print ('完成{}張影像，共{}筆影像錯誤'.format(len(img_list), len(error_url_list)))
#%%
img_np = np.empty((len(img_list), IMG_HEIGHT, IMG_WIDTH, 3))
img_np[:,...] = [np.array(i) for i in img_list] # 耗時50秒
#%%
%%time
hf["appids"][...] = app_list
hf["imgs"][...] = img_np #要轉成四維的np array才存得進去，不能直接塞list
print ('Header images {}張，存至hdf5完成'.format(len(app_list)))
hf.close() #close之後才會真的存檔

"""
np array大小(25153, 180, 320, 3)，存起來佔33GB (若先轉int再存只要16GB)
若存成hdf5，佔4.75GB。(先轉int再存，大小一樣，所以就不用先轉了)
"""
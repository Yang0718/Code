#%%
from distutils.log import error
from time import time
import requests
import json
import pandas as pd
import numpy as np
import read
# https://partner.steamgames.com/doc/store/getreviews

# games = pd.read_csv('games.csv',usecols=['AppID','Name'])
# summary = pd.read_csv('review_summary_asyncio.csv',index_col=0)
# df = pd.merge(games, summary, on='AppID')
# THR = 10
# df = df[(df['total_review_num']>=THR)].reset_index(drop=True)
df = read.readData(REVIEW_THRESHOLD=10)
appids = df.sort_values(by='total_review_num',ascending=0)['AppID'].tolist()
#%%
%%time
import urllib
import math
import urllib.request as req
NUM_PER_PAGE = 100

error_list = []
def do_requests(appid_l):
    appid_list, userid_list, review_list, like_list, refvalue_list, voteup_list  = [], [], [], [], [], []
    for appid in appid_l:
        try:
            num = summary[summary['AppID']==appid]['total_review_num'].values[0]
            # print ('共{}篇評論'.format(num))
            for i in range(math.ceil(num/NUM_PER_PAGE)): 
            # for i in range(5):
                if i == 0:
                    queryString = {
                                    'json' : 1,
                                    'filter' : 'updated',
                                    'cursor' : '*', # 不用特別傳。加了反而導致每次return的cursor都一樣，不知為啥
                                    'language': 'english',
                                    #    'purchase_type':'steam', # 由在 Steam 上自行購買產品的使用者撰寫（預設）
                                    'num_per_page':str(NUM_PER_PAGE) # 預設20，上限100。不知為啥設100會變成94篇，所以先設99
                                    }
                # print (i, '1 ',appid, cursor_dict[appid])
                query_code = urllib.parse.urlencode(queryString)
                url="https://store.steampowered.com/appreviews/"+str(appid)+"?" +query_code
                
                with req.urlopen(url) as response: 
                    data = json.load(response)
                    if queryString['cursor'] == data["cursor"]:
                        break
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

                    # print (1, queryString['cursor'] , data["cursor"])
                    if (queryString['cursor'] == data["cursor"]) or (data['query_summary']['num_reviews']<NUM_PER_PAGE):
                        # print (2, queryString['cursor'] , data["cursor"])
                        break # 代表再翻頁也沒東西了
                    else:
                        queryString['cursor'] = data["cursor"]
        except Exception as e:
            error_list.append(appid)
            print (appid, e, ' error.')
    reviews_df = pd.DataFrame()
    reviews_df["AppID"] = appid_list
    reviews_df["UserID"] = userid_list
    reviews_df["Like"] = like_list #是否喜歡
    reviews_df["RefValue"] = refvalue_list #評論的參考價值分數
    reviews_df["VoteUp"] = voteup_list #認為此篇評論值得參考的使用者人數
    reviews_df["Review"] = review_list
    return reviews_df
  
def get_and_save(appids_sliced, filename):
    reviews_df = do_requests(appids_sliced)
    reviews_df.to_pickle('reviews/ReviewsDF_{}.pkl'.format(filename))
    print ('Review總數：', df[df.AppID.isin(appids_sliced)]['total_review_num'].sum())
    print ('已成功爬取：',len(reviews_df))
    print ('重複：',reviews_df[reviews_df.duplicated(subset=['AppID','UserID'])].shape)

def job(i):
    appids_sliced = appids[i::SLICE]
    # appids_sliced = [292030]
    print ('第{}批開始...'.format(i))
    time.sleep(1)
    get_and_save(appids_sliced, i)
#%%
%%time
import threading 
import time
import concurrent.futures

SLICE = 3 # 開三個thread，共爬了13000萬筆，花7.5小時
with concurrent.futures.ThreadPoolExecutor(max_workers=SLICE) as executor:
    executor.map(job, range(SLICE))

if error_list:
    get_and_save(error_list, 'error')
#%%
# 存檔
df1 = pd.read_pickle('reviews/ReviewsDF_0.pkl')
df2 = pd.read_pickle('reviews/ReviewsDF_1.pkl')
df3 = pd.read_pickle('reviews/ReviewsDF_2.pkl')
df4 = pd.read_pickle('reviews/ReviewsDF_error.pkl')
df = pd.concat((df1, df2, df3, df4), axis=0)
print (df.shape)
print (df[df.duplicated(subset=['AppID','UserID'])].shape)
df = df.drop_duplicates(subset=['AppID','UserID']).reset_index(drop=True)
df.to_pickle('reviews.pkl')
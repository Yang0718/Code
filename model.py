#%%
from matplotlib.pyplot import text
import requests
import json
import numpy as np
import pandas as pd
import tqdm
import crawl.read as read

with open('Config.json') as j:
    config = json.load(j)

games = read.readData(REVIEW_THRESHOLD=config['APP_THRESHOLD'])
games['Required age'] = games['Required age'].replace(1,0).replace(10,12).replace(14,16).replace(15,16)

"""
Metacritic score: 國外媒體給遊戲打的平均評價(但有7成是0分,沒法用)
User score: 9成是0分,沒法用
Achievements: ??
Recommendations: ??
Notes: 註記, 例如有血腥暴力
"""

games = games[[ 'AppID',
                # categorical
                'Required age','Publishers','Genres','Tags','Supported languages',
                # numeric
                'Price','DLC count','Positive','Negative','Achievements',
                'Recommendations','Median playtime forever','Median playtime two weeks',
                # textual
                'About the game','Notes'
                ]]
#%%
# 標準化, dummy
games['About the game']
#%%
reviews = pd.read_pickle('crawl/data/reviews.pkl')
print (reviews.shape)
"""
方法一(2分鐘):
reviews.groupby(['UserID']).filter(lambda x : len(x)>3)

方法二(14秒):
reviews[reviews.groupby('UserID').UserID.transform('count')>3] # 僅需14秒

方法三(8秒):
counts = reviews['UserID'].value_counts()
reviews[reviews['UserID'].isin(counts[counts>3].index)]
"""
counts = reviews['UserID'].value_counts()
USER_THRESHOLD = config['USER_THRESHOLD'] # 每個user至少要發表過10篇評論
reviews = reviews[reviews['UserID'].isin(counts[counts>=USER_THRESHOLD].index)]
print ('限制每個user至少要有{}個評論：'.format(USER_THRESHOLD))
print ('共{}個user, {}筆評論'.format(len(counts[counts>=USER_THRESHOLD]), reviews.shape[0]))

games.reset_index(drop=True,inplace=True)
reviews.reset_index(drop=True,inplace=True)
#%%
# 文字前處理
def textPreprocess(df, textCol):
    df[textCol] = df[textCol].str.lower()
    replaceTexts = pd.read_csv('ReplaceTexts.csv',header=None)
    replaceTexts_dict = dict(zip(replaceTexts[0], replaceTexts[1]))
    df[textCol] = df[textCol].replace(replaceTexts_dict,regex=True)
    # for k,v in zip(replaceTexts['key'],replaceTexts['value']):
    #     reviews['Review'] = reviews['Review'].str.replace(k,v)




reviews['Review'] = reviews['Review'].str.lower()

replaceTexts = pd.read_csv('ReplaceTexts.csv')
for k,v in zip(replaceTexts['key'],replaceTexts['value']):
    reviews['Review'] = reviews['Review'].str.replace(k,v)
reviews['Review']
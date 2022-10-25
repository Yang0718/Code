#%%
import requests
import json
import pandas as pd
import tqdm
import read

games = read.readData(REVIEW_THRESHOLD=10)
genres_count = games['Genres'].str.split(',').explode('Genres').value_counts()
categories_count = games['Categories'].str.split(',').explode('Categories').value_counts()
# Genres較有參考價值
#%%
reviews = pd.read_pickle('data/reviews.pkl')
usercount = pd.DataFrame(reviews.groupby(['UserID']).size().sort_values())
#%%
print ('Games：',games.shape[0])
print ('Reviews：',reviews.shape[0])
print ('Unique userID：', reviews['UserID'].unique().shape[0])
print ('Ratio of like：', round(100*reviews[reviews.Like==1].shape[0] / reviews.shape[0],5),'%')
print ('Ratio of reference value = 0：',round(100*reviews[reviews['RefValue']==0].shape[0]/reviews.shape[0],3), '%')
#%%
like = reviews.groupby(['AppID'])['Like'].apply(lambda x: (x==True).sum())
total = reviews.groupby(['AppID'])['Like'].apply(lambda x: x.sum())
#%%
for i in range(1,11):
    print ('至少{}筆評論的user占總user數量的{}%,共{}個'.format(i, round(100*usercount[usercount[0]>=i].shape[0]/usercount.shape[0], 3),usercount[usercount[0]>=i].shape[0]))
for i in [20,30,40,50,100,200,300,400,500,1000]:
    print ('至少{}筆評論的user占總user數量的{}%,共{}個'.format(i, round(100*usercount[usercount[0]>=i].shape[0]/usercount.shape[0], 3),usercount[usercount[0]>=i].shape[0]))

#%%
import h5py
from PIL import Image
import numpy as np

# headers = h5py.File('headers.hdf5', 'r')
screenshots = h5py.File('screenshots.hdf5', 'r')
print ("兩個keys：appids, imgs")
i = Image.fromarray(np.uint8(screenshots['imgs'][100]))
print (screenshots['appids'][100])
i




# %%

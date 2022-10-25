import pandas as pd
import numpy as np
import sys
import os

def readData(REVIEW_THRESHOLD = 10):
    sys.path.append('..')
    # try:
    games = pd.read_csv(os.path.dirname(__file__)+'/data/games.csv')
    summary = pd.read_csv(os.path.dirname(__file__)+'/data/review_summary_asyncio.csv',index_col=0).drop(columns=['Genres'])
    # except:
    #     games = pd.read_csv('crawl/data/games.csv')
    #     summary = pd.read_csv('crawl/data/review_summary_asyncio.csv',index_col=0).drop(columns=['Genres'])
    df = pd.merge(games, summary, on='AppID')
    df = df[(df['total_review_num']>=REVIEW_THRESHOLD)].reset_index(drop=True)
    return df
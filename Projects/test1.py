#%%
%reload_ext autoreload
%autoreload 2
import sys
sys.path.append("..")
import alphalens
import pandas as pd

#对于my_factor, 需要先指定dtype={'asset':str}, 并且不能设置index_col,之后再set_index(否则asset会读成int)
my_factor=pd.read_csv('./data/my_factor.csv',dtype={'asset':str})
my_factor['date']=pd.to_datetime(my_factor['date'])
#set multiIndex [date,asset]
my_factor=my_factor.set_index(['date','asset'])

pricing=pd.read_csv('./data/pricing.csv',index_col=['date'],parse_dates=True)
#or equivlent:
#pricing['date']=pd.to_datetime(pricing['date'])
#pricing=pricing.set_index('date')

#%%
# Ingest and format data
factor_data = alphalens.utils.get_clean_factor_and_forward_returns(my_factor,
                                                                   pricing,
                                                                   quantiles=5,
                                                                   groupby=None,
                                                                   groupby_labels=None
                                                                   )
# Run analysis
alphalens.tears.create_full_tear_sheet(factor_data)
# %%
df=pd.read_csv('./cmb.csv',index_col=0,parse_dates=True)
df.head()
# %%
import numpy as np
from empyrical import (
    annual_return,
    max_drawdown,
    sharpe_ratio,
    sortino_ratio,
    calmar_ratio
)
t=df.apply([annual_return,max_drawdown,sharpe_ratio,sortino_ratio,calmar_ratio])
t.T
# %%

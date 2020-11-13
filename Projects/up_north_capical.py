#%%
# For debugging
# %reload_ext autoreload
# %autoreload 2
import sys
sys.path.append("..")
import alphalens
import pandas as pd
import pymssql
conn = pymssql.connect(host='192.168.0.144', user='readonly', password='readonly', database='jydb')

factor_path=r'C:\Projects\up_north_capital\data\hs300MHoldPER.csv'
pricing_path=r'C:\Projects\up_north_capital\data\Pricing.csv'

#对于my_factor, 需要先指定dtype={'asset':str}, 并且不能设置index_col,之后再set_index(否则asset会读成int)
my_factor=pd.read_csv(factor_path,dtype={'asset':str})
my_factor['date']=pd.to_datetime(my_factor['date'])
my_factor=my_factor.set_index(['date','asset'])

pricing=pd.read_csv(pricing_path,index_col=['date'],parse_dates=True)
#or equivlent:
#pricing['date']=pd.to_datetime(pricing['date'])
#pricing=pricing.set_index('date')

market_index_name='000300'

sql='''
select a.TradingDay, a.ClosePrice from jydb.dbo.QT_IndexQuote a
left join jydb.dbo.SecuMain b
on a.InnerCode = b.InnerCode
where b.SecuCode = '000300' and a.TradingDay >= '{}' and a.TradingDay <= '{}'
order by a.TradingDay
'''.format(pricing.index.min(),pricing.index.max())

market_data=pd.read_sql(sql,conn)
conn.close()
market_data=market_data.set_index('TradingDay')
market_data.index.name='date'
#%%
# Ingest and format data
factor_data = alphalens.utils.get_clean_factor_and_forward_returns(my_factor,
                                                                   pricing,
                                                                   quantiles=5,
                                                                   groupby=None,
                                                                   groupby_labels=None
                                                                   )
market_data1=alphalens.utils.compute_market_index_forward_returns(my_factor,market_data)
# Run analysis
alphalens.tears.create_full_tear_sheet(factor_data,index_name=market_index_name,market_index=market_data1)
# %%

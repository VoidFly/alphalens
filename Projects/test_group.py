#%%
# For debugging
%reload_ext autoreload
%autoreload 2
import sys
sys.path.append("..")
import alphalens
import pandas as pd
import pymssql
conn = pymssql.connect(host='192.168.0.144', user='readonly', password='readonly', database='jydb')

factor_path='./data/my_factor.csv'
prices_path='./data/pricing.csv'


#对于my_factor, 需要先指定dtype={'asset':str}, 并且不能设置index_col,之后再set_index(否则asset会读成int)
my_factor=pd.read_csv(factor_path,dtype={'asset':str},sep=r'\s*,\s*')
my_factor['date']=pd.to_datetime(my_factor['date'])
#set multiIndex [date,asset]
my_factor=my_factor.set_index(['date','asset'])

prices=pd.read_csv(prices_path,index_col=['date'],parse_dates=True)
#or equivlent:
#prices['date']=pd.to_datetime(prices['date'])
#prices=prices.set_index('date')

market_index_name='000300'

sql='''
select a.TradingDay, a.ClosePrice from jydb.dbo.QT_IndexQuote a
left join jydb.dbo.SecuMain b
on a.InnerCode = b.InnerCode
where b.SecuCode = '000300' and a.TradingDay >= '{}' and a.TradingDay <= '{}'
order by a.TradingDay
'''.format(prices.index.min(),prices.index.max())

market_data=pd.read_sql(sql,conn)
conn.close()
market_data=market_data.set_index('TradingDay')
market_data.index.name='date'
market_data=market_data.shift(-1)


#%%#dict-> group_code:group_name为了在画图中显示行业名称
# Ingest and format data
factor_data = alphalens.utils.get_clean_factor_and_forward_returns( my_factor,
                                                                    prices,
                                                                    groupby=None,#pd.Series or dict
                                                                    binning_by_group=False,
                                                                    quantiles=5,
                                                                    bins=None,
                                                                    periods=(1, 5, 10),
                                                                    filter_zscore=20,
                                                                    groupby_labels=None,
                                                                    max_loss=0.35,
                                                                    zero_aware=False,
                                                                    cumulative_returns=True)




market_data1=alphalens.utils.compute_market_index_forward_returns(my_factor,market_data,periods=[1,2,5,10])
#%%
# Run analysis
#group_neutral指是否对每一个group的收益率demean，需要factor_data里面有group标记
#long_short指是否对所有收益率demean
alphalens.tears.create_full_tear_sheet(factor_data,index_name=market_index_name,market_index=market_data1,group_neutral=False,long_short=False)
#alphalens.tears.create_information_tear_sheet(factor_data)
# %%
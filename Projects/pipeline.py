#%%
%reload_ext autoreload
%autoreload 2
import sys
sys.path.append("..")
from alphalens import alphalens
import pandas as pd
import pymssql
import numpy as np
from scipy import stats
import statsmodels.api as sm 
from datetime import datetime
import datetime as dt
from tqdm import tqdm
pd.set_option('display.max_rows',800)
pd.set_option('display.max_columns',None)

begin_date=datetime.strptime('20200102','%Y%m%d')
end_date=datetime.strptime('20201215','%Y%m%d')

#%%
# read factors (daily factors)
factors=pd.read_csv('./factors.csv',index_col=0,dtype={'wind_code':str},parse_dates=['date']).reset_index(drop=True)
factors=factors.rename(columns={'wind_code':'asset'})
factors=factors.drop_duplicates(subset=['date','asset'],keep='last')
stock_list=tuple(factors['asset'].unique().tolist())
#%%
# get prices
conn = pymssql.connect(host='192.168.0.144', user='readonly', password='readonly', database='jydb')
sql='''
select a.InnerCode, b.SecuCode, a.TradingDay, a.ClosePrice, a.NegotiableMV, a.Ifsuspend from jydb.dbo.QT_StockPerformance as a
left join jydb.dbo.SecuMain b
on a.InnerCode = b.InnerCode
where b.SecuCode in {} and a.TradingDay >= '{}' and a.TradingDay <= '{}'
'''.format(stock_list,begin_date,end_date)
prices=pd.read_sql(sql,conn)
prices['TradingDay']=pd.to_datetime(prices['TradingDay'])
conn.close()
prices['logNegotiableMV']=np.log(prices['NegotiableMV'])
prices=prices.rename(columns={'SecuCode':'asset','TradingDay':'date','ClosePrice':'close'})
prices
#%%
#获取行业数据
conn = pymssql.connect(host='192.168.0.144', user='readonly', password='readonly', database='jydb')
sql='''
select b.SecuCode,a.InfoPublDate,a.FirstIndustryCode,a.FirstIndustryName from jydb.dbo.LC_ExgIndustry a
left join jydb.dbo.SecuMain b
on a.CompanyCode =b.CompanyCode 
where b.SecuCategory =1 and a.Standard = 24
'''
df_industry=pd.read_sql(sql,conn)
df_industry['FirstIndustryName'] = df_industry.FirstIndustryName.str.encode('latin1').str.decode('gbk')
conn.close()
df_industry=df_industry.rename(columns={'FirstIndustryCode':'group_code','FirstIndustryName':'group_name','SecuCode':'asset','InfoPublDate':'date'})
df_industry

#%%
#get market index
conn = pymssql.connect(host='192.168.0.144', user='readonly', password='readonly', database='jydb')
market_index_name='000300'
sql='''
select a.TradingDay, a.ClosePrice from jydb.dbo.QT_IndexQuote a
left join jydb.dbo.SecuMain b
on a.InnerCode = b.InnerCode
where b.SecuCode = '000300' and a.TradingDay >= '{}' and a.TradingDay <= '{}'
order by a.TradingDay
'''.format(begin_date,end_date+dt.timedelta(days=1))#确保时间区间大于等于factors的时间区间，避免出现nan
market_data=pd.read_sql(sql,conn)
conn.close()
market_data=market_data.set_index('TradingDay')
market_data.index.name='date'

#%%
def process_factors_and_prices(factors,prices,freq_str,ffill_limit=23,
                                groupby=None,
                                NegotiableMV_neutral=False,
                                group_neutral=False
                                ):
    '''
    factors-> pd.DataFrame
    |date|asset|...factors...| 
    prices-> pd.DataFrame
    |date|asset|close|[OPTIONAL]logNegotiableMV|
    groupby-> pd.DataFrame
    |date|asset|group_code|[OPTIONAL] group_name|
    freq_str-> eg '1D' 'W'

    note: price 为实际当日收盘价，alphalens_run中会对prices和market_index shift-1
    '''
    factor_list=factors.columns.drop(['date','asset'])

    def neutralize(endog,exog_continues,exog_catgorical):
        '''
        1. make sure to drop nans
        2. if pd.Series is passed, make sure the index is reseted
        '''
        dummy=sm.categorical(exog_catgorical,drop=True)
        dummy=pd.DataFrame(dummy)
        X=pd.concat([exog_continues,dummy],axis=1)
        ols_model=sm.OLS(endog,X)
        result=ols_model.fit()
        r=result.resid
        return r

    if NegotiableMV_neutral and group_neutral:
        #添加流通市值
        df=pd.merge(factors,prices[['asset','date','logNegotiableMV']],on=['date','asset'],how='inner')
        #添加行业代码
        if groupby is None:
            raise KeyError('No groupby dataframe passed')
        df=df.sort_values(by=['date','asset'])
        groupby=groupby.sort_values(by=['date','asset'])
        df=pd.merge_asof(df,groupby,on='date',by='asset')
        df=df.dropna().reset_index(drop=True)
    else:
        df=factors

    period_df=pd.DataFrame()
    for this_period,t in df.resample(freq_str,on='date'):
        if t.empty:
            continue
        t=t.reset_index(drop=True)
        last_day=t['date'].max()
        t=t[t['date'] == last_day].reset_index(drop=True)

        if NegotiableMV_neutral and group_neutral:
            for factor in factor_list:
                t[factor]=neutralize(t[factor],t['logNegotiableMV'],t['group_code'])

        period_df=period_df.append(t)

    period_df_filled=pd.DataFrame()
    for this_stock,this_stock_df in period_df.groupby(['asset']):
        this_stock_df=this_stock_df.sort_values(by=['date'])
        t=prices[prices['asset'] == this_stock].reset_index(drop=True)[['asset','date','close']]
        merged=pd.merge(t,this_stock_df,on=['date','asset'],how='left')
        merged=merged.fillna(axis=0,method='ffill',limit=ffill_limit)
        period_df_filled=period_df_filled.append(merged)
        
    period_df_filled=period_df_filled.set_index(['date','asset']).sort_index(level=[0,1])
    period_df_filled=period_df_filled.dropna()
    return period_df_filled,factor_list
    #period_df_filled
#%%
def alphalens_run(factor_path,prices_path,
                    periods=(1,5,20),
                    market_index=None,market_index_name='market_mean',
                    group_code_path=None,group_name_path=None,
                    by_group=False,
                    long_short=False,
                    group_neutral=False):
    '''
    by_group: 是否分行业计算IC，分行业计算分组mean_return
    group_neutral: 是否对每个行业内的收益demean
    '''
    #get factor
    factors=pd.read_csv(factor_path,dtype={'asset':str},parse_dates=['date'])
    factors=factors.set_index(['date','asset'])
    #get pricing
    prices=pd.read_csv(prices_path,parse_dates=['date'])
    prices=prices.shift(-1)
    prices=prices.set_index(['date'])
    #get groupby
    group_name=None
    if group_code_path is not None:
        group_code=pd.read_csv(group_code_path,parse_dates=['date'],dtype={'asset':str})
        group_code=group_code.set_index(['date','asset'])
        group_code=group_code['group_code']
        if group_name_path is not None:
            #generate dict group_code:group_name
            tmp=pd.read_csv(group_name_path,parse_dates=['date'],dtype={'asset':str})
            tmp=tmp.set_index(['date','asset'])
            tmp['group_code']=group_code
            tmp=tmp.dropna()
            group_name=dict(zip(tmp['group_code'].tolist(),tmp['group_name'].tolist()))
    else:
        group_code=None
    # Ingest and format data
    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(factors,
                                                                    prices,
                                                                    quantiles=5,
                                                                    periods=periods,
                                                                    groupby=group_code,
                                                                    groupby_labels=group_name,
                                                                    )
    if market_index is not None:
        market_index=market_index.shift(-1)
        market_index=alphalens.utils.compute_market_index_forward_returns(factors,market_index,periods=periods)
    #Run analysis
    alphalens.tears.create_full_tear_sheet(factor_data,
                                            index_name=market_index_name,
                                            market_index=market_index,
                                            by_group=by_group,
                                            long_short=long_short,
                                            group_neutral=group_neutral)
    #alphalens.tears.create_summary_tear_sheet(factor_data,long_short=False)

#%%
# process and save data
df,factor_list=process_factors_and_prices(factors,prices,'W',groupby=df_industry,NegotiableMV_neutral=True,group_neutral=True)
for factor in factor_list:
    df[[factor]].to_csv('./data/weekly/{}.csv'.format(factor))
df['group_code'].to_csv('./data/weekly/group_code.csv')
df['group_name'].to_csv('./data/weekly/group_name.csv')
prices_expand=df['close'].unstack()
prices_expand.to_csv('./data/weekly/Prices.csv')
#%%
#run alphalens
prices_path=r'C:\Projects\High_freq_factors\data\weekly\prices.csv'
group_code_path=r'C:\Projects\High_freq_factors\data\weekly\group_code.csv'
group_name_path=r'C:\Projects\High_freq_factors\data\weekly\group_name.csv'
for i in factor_list[2:3]:
    print(i)
    factor_path=r'C:\Projects\High_freq_factors\data\weekly\{}.csv'.format(i)
    alphalens_run(factor_path,prices_path,
                market_index=market_data,
                market_index_name='000300',
                group_code_path=group_code_path,
                group_name_path=group_name_path,
                by_group=True)#,group_code_path,group_name_path)
# %%

#%%
import cProfile
import pstats
import sys
sys.path.append("..")
import alphalens
import pandas as pd

my_factor=pd.read_csv('./data/my_factor.csv',dtype={'asset':str})
my_factor['date']=pd.to_datetime(my_factor['date'])
#set multiIndex [date,asset]
my_factor=my_factor.set_index(['date','asset'])
pricing=pd.read_csv('./data/pricing.csv')
pricing['date']=pd.to_datetime(pricing['date'])
pricing=pricing.set_index('date')
#%%
# Ingest and format data
factor_data = alphalens.utils.get_clean_factor_and_forward_returns(my_factor,
                                                                   pricing,
                                                                   quantiles=5,
                                                                   groupby=None,
                                                                   groupby_labels=None
                                                                   )

# Run analysis
def run_analysis():
    alphalens.tears.create_full_tear_sheet(factor_data)

#%%
p = cProfile.Profile()
p.runcall(run_analysis)
p.dump_stats('run_analysis_profile.stats')

stats = pstats.Stats('run_analysis_profile.stats')
stats.sort_stats('cumtime').print_stats(50)
# %%

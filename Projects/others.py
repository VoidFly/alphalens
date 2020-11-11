
# %%
df=pd.read_csv('./cmb.csv',index_col=0,parse_dates=True)
df.head()
# %%
import empyrical as ep
ep.__file__
# %%
a=pd.DataFrame([10,20,5,40,30])
print(a.pct_change())
print((a.pct_change()+1).fillna(1).values.cumprod())
# %%
#%%
a=pd.DataFrame([10,20,5,40,30]).pct_change().fillna(1)
def cum_returns(returns, starting_value=0, out=None):
    """
    Compute cumulative returns from simple returns.

    Parameters
    ----------
    returns : pd.Series, np.ndarray, or pd.DataFrame
        Returns of the strategy as a percentage, noncumulative.
         - Time series with decimal returns.
         - Example::

            2015-07-16   -0.012143
            2015-07-17    0.045350
            2015-07-20    0.030957
            2015-07-21    0.004902

         - Also accepts two dimensional data. In this case, each column is
           cumulated.

    starting_value : float, optional
       The starting returns.
    out : array-like, optional
        Array to use as output buffer.
        If not passed, a new array will be created.

    Returns
    -------
    cumulative_returns : array-like
        Series of cumulative returns.
    """
    if len(returns) < 1:
        return returns.copy()

    nanmask = np.isnan(returns)
    if np.any(nanmask):
        returns = returns.copy()
        returns[nanmask] = 0

    allocated_output = out is None
    if allocated_output:
        out = np.empty_like(returns)

    np.add(returns, 1, out=out)
    out.cumprod(axis=0, out=out)

    if starting_value == 0:
        np.subtract(out, 1, out=out)
    else:
        np.multiply(out, starting_value, out=out)

    if allocated_output:
        if returns.ndim == 1 and isinstance(returns, pd.Series):
            out = pd.Series(out, index=returns.index)
        elif isinstance(returns, pd.DataFrame):
            out = pd.DataFrame(
                out, index=returns.index, columns=returns.columns,
            )

    return out
def max_drawdown(returns, out=None):
    """
    Determines the maximum drawdown of a strategy.

    Parameters
    ----------
    returns : pd.Series or np.ndarray
        Daily returns of the strategy, noncumulative.
        - See full explanation in :func:`~empyrical.stats.cum_returns`.
    out : array-like, optional
        Array to use as output buffer.
        If not passed, a new array will be created.

    Returns
    -------
    max_drawdown : float

    Note
    -----
    See https://en.wikipedia.org/wiki/Drawdown_(economics) for more details.
    """
    allocated_output = out is None
    if allocated_output:
        out = np.empty(returns.shape[1:])

    returns_1d = returns.ndim == 1

    if len(returns) < 1:
        out[()] = np.nan
        if returns_1d:
            out = out.item()
        return out

    returns_array = np.asanyarray(returns)

    cumulative = np.empty(
        (returns.shape[0] + 1,) + returns.shape[1:],
        dtype='float64',
    )
    cumulative[0] = start = 100
    cum_returns(returns_array, starting_value=start, out=cumulative[1:])

    max_return = np.fmax.accumulate(cumulative, axis=0)

    nanmin((cumulative - max_return) / max_return, axis=0, out=out)
    if returns_1d:
        out = out.item()
    elif allocated_output and isinstance(returns, pd.DataFrame):
        out = pd.Series(out)

    return out
max_drawdown(a)
#%%
#%%
import numpy as np
import matplotlib.pyplot as plt

x = [100, 200, 50, 300, 150, 100, 200]
j = np.argmax(np.maximum.accumulate(x) - x)
print('j is {j}'.format(j=j))
i = np.argmax(x[:j])
print('i is {i}'.format(i=i))
d = x[i] - x[j]
print(d)

plt.plot(x)
plt.plot([i, j], [x[i], x[j]],
         'o', color='Red', markersize=10)
plt.show()
# %%

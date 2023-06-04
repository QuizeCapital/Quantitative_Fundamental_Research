## `Calulations`
## Calculation of Daily Returns and Yearly Cumulative Percentage Change

To calculate the daily returns and yearly cumulative percentage change, you can use the following formulas:

1. Calculate Daily Returns:
   ```python
   df['daily_return'] = df.groupby(['symbol', 'year'])['close'].pct_change()

2. Calculate Yearly Cumulative Percentage Change:
    ```python
    df['cum_pct_ch_year'] = df.groupby(['symbol', 'year'])['daily_return'].sum().mul(100)
    ```






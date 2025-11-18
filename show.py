import pandas as pd
df = pd.read_csv('data/raw/apps_basic_*.csv')
print(df.head())
print(df.columns)
print(df['category'].value_counts())

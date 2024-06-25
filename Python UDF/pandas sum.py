import pandas as pd

data_values = [['IN',100],['PK',10],['AU',110],['IN',110],['PK',5],['AU',10]]
cols = ['country','sales']

df = pd.DataFrame(data=data_values,columns=cols)

print(df)
print()

df2 = df.groupby('country').sum()
print(df2)

df3 = df.groupby('country')['sales'].sum()
print(df3)

data_values2 = [['IN',100,1000],['PK',10,100],['AU',110,10],['IN',110,1100],['PK',5,50],['AU',10,15]]
cols2 = ['country','sales','population']

df4 = pd.DataFrame(data=data_values2,columns=cols2)

df5 = df4.groupby('country').sum()
print(df5)

df6 = df4.groupby('country')['sales'].sum()
print(df6)

df7 = df4.groupby('country')['population'].mean()
print(df7)

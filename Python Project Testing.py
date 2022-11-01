import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import missingno as msn
import re


def CheckColumnNaNs(dfcol):
    count_nan = dfcol.isnull().sum()
    return count_nan


def Dummy_Char(df):
    df = df.fillna(0)
    df[df != 0] = 1
    return df


def ChangeColumnNaNs(dfcol, repl):
    for i in dfcol.values():
        if i.isnull():
            i = repl
    return dfcol


def get_key(val, my_dict):
    for key, value in my_dict.items():
        if val == value:
            return key


def fix_the_price(col):
    col = col.replace("$","")
    col = col.replace(",", "")
    col = int(col)
    return col

#ETL Process



data = pd.read_csv(r'F:\Work\Projects\Airbnb\Airbnb_Open_Data.csv', low_memory=False)

print("Before dropping NaN rows:", data.shape)
#Dummy Variable
data['house_rules'] = Dummy_Char(data['house_rules'])


# Empty Column
data = data.drop('license', axis=1)


# Changing NaNs
for index, value in data['NAME'].items():
    if pd.isnull(data['NAME'][index]):
        data['NAME'][index] = data['id'][index]


# Group Column Data transforming
data['neighbourhood group'] = data['neighbourhood group'].replace(['brookln', 'manhatan'], ['Brooklyn', 'Manhattan'])


#  Retrieving Neighbourhood Group Missing Data
dictionary = data[['neighbourhood group', 'neighbourhood']].dropna()
dictionary = dictionary.groupby('neighbourhood')['neighbourhood group'].apply(lambda x: np.unique(x))
dictionary = {k: dictionary[k] for k in dictionary.keys()}
for index, value in data['neighbourhood group'].items():
    if value is None:
      value = dictionary[data.loc[:, ('neighbourhood', index)]]


# Droping non-contributing columns
data.drop(['country','country code'],axis=1)


# Removing NaNs
data = data.dropna()


# After removing NaNs
data.loc[:, 'Construction year']= data['Construction year'].apply(lambda x: int(x))
data.loc[:, 'price'] = data.loc[:, 'price'].apply(fix_the_price)
data.loc[:, 'service fee'] = data.loc[:, 'service fee'].apply(fix_the_price)
data.loc[:, 'instant_bookable'] = data.loc[:, 'instant_bookable'].apply(lambda x: 1 if x == True else 0)
data.loc[:, 'host_identity_verified'] = data.loc[:, 'host_identity_verified'].apply(lambda x: 0 if x == 'unconfirmed' else 1)
data.loc[:, 'last review'] = pd.to_datetime(data['last review'], dayfirst=True)
print("After dropping NaN rows:", data.shape)

STDVsf = data['service fee'].std(axis=0,skipna=True)
AVGsf = data['service fee'].mean(axis=0,skipna=True)
data['Z-SCORE'] = data['service fee'].apply(lambda x: (x-AVGsf)/STDVsf)
Z_SCOREsf = data[['id','Z-SCORE']]
print(AVGsf,STDVsf)
Z_SCOREsf.to_csv(r'F:\Work\Projects\Airbnb\Z_Scoresf.csv')
#Analysis

 #1) Customer Background

 #2) House Market Analysis
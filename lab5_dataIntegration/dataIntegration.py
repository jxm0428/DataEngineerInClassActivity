import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)

covid_data = pd.read_csv('COVID_county_data.csv')
census_data = pd.read_csv('acs2017_census_tract_data.csv')

covid_data['date'] = pd.to_datetime(covid_data['date'], format='%Y-%m-%d')
covid_data['county'] = covid_data['county'] + ' County'
feb20total = covid_data[(covid_data['date'] == '2021-02-20')]
dec2020total = covid_data[(covid_data['date'] >= '2020-01-01') & (covid_data['date'] <= '2020-12-28')]

censusCol = {
    'State':census_data['State'],
    'County':census_data['County'],
    'Population':census_data['TotalPop'],
    'Poverty%':census_data['Poverty']*census_data['TotalPop']/100,
    'PerCapitalIncome':census_data['IncomePerCap']
}
covidCol = {
    'State':covid_data['state'],
    'County':covid_data['county'],
    'Total Cases 02/20/2021':feb20total['cases'],
    'Total Deaths 02/20/2021':feb20total['deaths'],
    'Dec Cases':dec2020total['cases'],
    'Dec Deaths':dec2020total['deaths']
}

df1 = pd.DataFrame.from_dict(censusCol)
df2 = pd.DataFrame.from_dict(covidCol)

result1 = df1.groupby(['State','County']).sum().reset_index()
result1['Poverty%'] = result1['Poverty%']/result1['Population']*100
result2 = df2.groupby(['State','County']).sum().reset_index()
#print(result2[:10])
total = pd.merge(result1,result2,how='left',on=['State','County'])
oregon = total[total['State'].isin(['Oregon'])]
print(oregon)
#print(total[:10])


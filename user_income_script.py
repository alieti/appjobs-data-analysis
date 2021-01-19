'''Inferences on user income.

@author: Ali Etminan

This scripts uses raw data fram AppJobs database to make inferences
on the average income users earn on AppJobs.

'''
# IMPORT MODULES --------------------------------------------------------------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# SETUP CONNECTION ------------------------------------------------------------

# reading in data
conn_str = 'CREDENTIALS_TO_ACCESS_DB'
engine = create_engine(conn_str)
connection = engine.connect()

# SQL Query to retrieve required user data ------------------------------------

query = """

select ujo.created_at as time,
	pt.name as platform,
	usr.id as user,
	ujo.job_offer_id as offer,
	jo.salary as salary,
	ujo.currency as currency
from user_job_offers ujo
	join job_offers jo 
	on ujo.job_offer_id = jo.id 
	join auth0_users usr
	on ujo.user_id = usr.id
	join partners pt
	on jo.partner_id = pt.id

"""
data = pd.read_sql(query, connection)

# calculating average salary for offers with salary range
data['salary'] = data['salary'].astype(str)\
    .str.replace('+','')\
        .str.split('-', expand=True)\
            .apply(lambda x: pd.to_numeric(x, errors='coerce'))\
                .mean(axis=1)


# separating by currency type to convert into USD
data_euro = data[data['currency'] == 'EUR']
data_usd = data[data['currency'] == 'USD']
data_sek = data[data['currency'] == 'SEK']

# converting into USD by average FX rate
data_euro['salary'] = data_euro['salary']*1.17
data_sek['salary'] = data_sek['salary']*0.11

# Reconstructing dataframe (rows with currency= NaN are eliminated)
data = [data_euro,data_usd,data_sek]
data = pd.concat(data)
data = data.drop(columns='currency')

# converting to np.array to get statistics
income = data.groupby('user').salary.sum().reset_index()
income = income[income['salary'] < 1000]
income_np = income.to_numpy()


print(np.std(income_np[:,1]))
print(np.mean(income_np[:,1]))
print(np.median(income_np[:,1]))
print(np.max(income_np[:,1]))


sns.set_style('whitegrid')
sns.distplot(income.iloc[:,1], hist=False, kde=True,
             kde_kws = {'shade': True, 'linewidth': 3})
plt.title('Distribution of income for < 1000 USD')
plt.xlabel('Income')
plt.ylabel('Density')


# breaking down income by number of offers
inc_offer = data.groupby('user').agg({'salary' : 'sum',
                                      'offer' : 'count'}).reset_index()

inc_offer = inc_offer[inc_offer['salary'] < 1000]

pltinc = inc_offer.sample(n=5000, random_state=123)
sns.kdeplot(pltinc.offer, pltinc.salary)
plt.title('Joint density of number of offers with average salary')


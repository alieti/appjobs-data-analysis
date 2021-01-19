'''Appjobs partners data script.

@author: Ali Etminan

This scripts pipelines raw data from AppJobs database, calculates metrics 
on the performance of companies' that post jobs on AppJobs website and 
pushes the new metrics table back to the database. 

'''

# IMPORT MODULES --------------------------------------------------------------

import pandas as pd
from sqlalchemy import create_engine

# SETUP CONNECTION ------------------------------------------------------------

conn_str = 'CREDENTIALS_TO_ACCESS_DB'
engine = create_engine(conn_str)
connection = engine.connect()

# SQL Query to retrieve partners dataset --------------------------------------

query = """

    SELECT ujo.created_at AS date,
       ct.name AS city,
       cnt.name AS country,
       pt.name AS platform,
       ujo.user_id AS user,
       ujo.job_offer_id AS offer_id,
       ujo.cpc AS cpc,
       ujo.cpa AS cpa,
       ujo.weight AS weight,
       ujo.budget_exhausted AS budget_spent,
       pt.brand_id AS brand_id,
       jo.rating AS rating,
       jo.low_on_cost_rating AS low_on_cost_rating
    FROM partners as pt
    JOIN job_offers as jo
        ON jo.partner_id = pt.id
    JOIN user_job_offers as ujo
        ON ujo.job_offer_id = jo.id
    JOIN cities as ct
        ON jo.city_id = ct.id
    JOIN countries AS cnt
        ON ct.country_id = cnt.id

    """
    
# LOAD DATA -------------------------------------------------------------------

partners_source = pd.read_sql(query, connection)

# TRANSFORM -------------------------------------------------------------------

# start by changing datetime to date to reduce granularity

partners_source['year_week'] = (pd.to_datetime(partners_source['date'])
                                  .dt.strftime('%Y-%U'))

partners_source['click'] = (pd.to_datetime(partners_source['date']).\
                            dt.strftime('%B %d, %Y, %r') +
                            "_" +
                            partners_source['user'] +
                            "_" +
                            partners_source['offer_id'].apply(str))

# Caclulating daily loyalty
ps1 = partners_source[['year_week','platform','country','user','offer_id']]

ps2 = (ps1.groupby(['year_week','platform','country','user'])
          .offer_id
          .count()
          .to_frame())
ps3 = ps2.groupby('user').sum()
ps4 = (ps2.join(ps3, on = 'user', how = 'inner', lsuffix = '_l', rsuffix = '_r')
          .reset_index())
ps4['loyalty'] = ps4['offer_id_l']/ps4['offer_id_r']
ps5 = (ps4.groupby(['platform','country','year_week'])
          .loyalty
          .mean()
          .round(2)
          .reset_index())


# Calculating long term loyalty
ps5['longterm_loyalty']= (ps5.groupby(['platform', 'country'])
                          .loyalty
                          .expanding(min_periods=1)
                          .mean()
                          .round(2)
                          .reset_index(drop=True))
loyalty =  ps5.set_index(['platform','country','year_week'])
# PERFORMANCE -----------------------------------------------------------------

# create aggregation function
def performance(data):
    """ Performance metrics for the partners companies

    The function aggregates the partners_source table

    Parameters
    ----------
    data : pandas DataFrame
        Name of the DataFrame that will be used for the calculation.
    Returns
    -------
    an aggregated pandas DataFrame 
    """
    users = len(data.user.unique())
    clicks = len(data.click.unique())
    rating = round(data.rating.mean(), 2)
    return pd.Series({'users': users,
                      'clicks': clicks,
                      'rating': rating})

partners_group = partners_source.groupby(['platform',
                                          'country',
                                          'year_week']).apply(performance)


# partners_group = partners_group.reset_index()
partners_group = partners_group.sort_values(['platform','country','year_week'])


partners_group['weekly_user_growth'] = (partners_group.groupby(['platform',
                                                               'country'])['users']
                                                     .pct_change()
                                                     .round(2))

partners_group['weekly_click_growth'] = (partners_group.groupby(['platform',
                                                                'country'])['clicks']
                                                      .pct_change()
                                                      .round(2))

partners_group['user_growth'] = (partners_group.groupby(['platform',
                                                  'country'])['weekly_user_growth']
                                                 .apply(lambda x: x + 1))


partners_group['user_growth'] = (partners_group.groupby(['platform',
                                                  'country'])['user_growth']
                                               .cumprod()
                                               .round(2))


partners_group['click_growth'] = (partners_group.groupby(['platform',
                                                         'country'])['weekly_click_growth']
                                                .apply(lambda x: x + 1))


partners_group['click_growth'] = (partners_group.groupby(['platform',
                                                  'country'])['click_growth']
                                               .cumprod()
                                               .round(2))

partners_group['weekly_user_growth'] = partners_group['weekly_user_growth']*100 

partners_group['weekly_click_growth'] = partners_group['weekly_click_growth']*100 

partners_group['user_growth'] = partners_group['user_growth']*100

partners_group['click_growth'] = partners_group['click_growth']*100


# merging all performance scores
partners_group = partners_group.merge(loyalty,
                               left_index=True,
                               right_index=True)

partners_agg = partners_group.reset_index()
# anonymizing
partners_anonymized = partners_agg[partners_agg['users'] > 30].reset_index()
partners_anonymized = partners_anonymized.drop(columns='index')


# PUSH TABLE TO DB -------------------------------------------------------------

partners_anonymized.to_sql('partners', 
                           connection, schema='dev', if_exists='replace')

connection.close()



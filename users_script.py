'''Appjobs partners data script.

@author: Ali Etminan

This scripts pipelines raw data from AppJobs database, calculates metrics 
based on data from users (job seekers) on AppJobs website and  pushes the new 
metrics table back to the database. 

'''

# IMPORT MODULES ---------------------------------------------------------------

# analysis
import pandas as pd
from sqlalchemy import create_engine

# SET PATHS -------------------------------------------------------------------

conn_str = 'CREDENTIALS_TO_ACCESS_DB'
engine = create_engine(conn_str)
connection = engine.connect()

# SQL Query to retrieve User  dataset -----------------------------------------

query = """

SELECT vw.created_at AS date,
       usr.id AS user,
       vw.type AS view_type,
       vw.job_offer_id AS offer_id,
       CASE WHEN ujo.job_offer_id = vw.job_offer_id 
             AND ujo.user_id = vw.user_id
             THEN TRUE ELSE FALSE
             END AS clicked,
       ujo.weight AS cpc_sek,
       pt.name AS platform,
       ct.name AS city,
       cnt.name AS country,
       jo.rating AS offer_rating,
       vw.locale AS language
FROM views as vw
    JOIN auth0_users AS usr
        ON vw.user_id = usr.id
    LEFT JOIN user_job_offers as ujo 
        ON vw.user_id = ujo.user_id
        and vw.job_offer_id = ujo.job_offer_id
    left JOIN partners AS pt
        ON vw.partner_id = pt.id
    left JOIN cities AS ct
        ON vw.city_id = ct.id
    JOIN countries AS cnt
        ON ct.country_id = cnt.id
    left JOIN job_offers AS jo
        ON vw.job_offer_id = jo.id
    
"""

    
# LOAD DATA --------------------------------------------------------------------

users_raw = pd.read_sql(query, connection)

# TRANSFORM --------------------------------------------------------------------

# start by changing datetime to date to reduce granularity
users_raw['year_week'] = pd.to_datetime(users_raw['date']).dt.strftime('%Y-%U')

# PERFORMANCE ------------------------------------------------------------------

# compute column of unique platforms by user
plt_per_user = users_raw.groupby(['year_week',
                                  'country',
                                  'city',
                                  'user'])['platform'].nunique()
plt_per_user_avg = plt_per_user.groupby(['year_week',
                                         'country',
                                         'city']).mean().round(2)

# create aggregation function  
def performance(data):
    """ Calculates performance metrics for Appjobs user

    The function aggregates the users_raw table

    Parameters
    ----------
    data : pandas DataFrame
        Name of the DataFrame that will be used for the calculation.
    Returns
    -------
    an aggregated pandas DataFrame 
    """
    unique_users = len(data.user.unique())
    total_clicks = data.clicked.sum()
    total_views = len(data.index)
    ctr = round(total_clicks / total_views, 2)
    views_per_user =  round(total_views / unique_users, 2)
    offer_rating = round(data.offer_rating.mean(), 2)
    return pd.Series({'unique_users': unique_users,
                      'total_click': total_clicks,
                      'total_views': total_views,
                      'ctr': ctr,
                      'views_per_user': views_per_user,
                      'offer_rating': offer_rating})

users_group = users_raw.groupby(['year_week',
                                 'country',
                                 'city']).apply(performance)

# merging all performance scores
users_group = users_group.merge(plt_per_user_avg.rename('platforms_per_user'),
                               left_index=True,
                               right_index=True)
users_agg = users_group.reset_index()

# Remove all users that are to unique in there characteristics
users_anonymized = users_agg[users_agg['unique_users'] > 20].reset_index()
users_anonymized = users_anonymized.drop(columns='index')


# Export to DB -------------------------------------------------------------

users_anonymized.to_sql('users', connection, schema='dev', if_exists='replace')

connection.close()

'''Appjobs job offers data script.

@author: Ali Etminan

This scripts pipelines raw data from AppJobs database, calculates metrics 
based on job offers/gig offers published on AppJobs website and pushes the
new metrics table back to the database. 

'''

# IMPORT MODULES ---------------------------------------------------------------

import pandas as pd
from sqlalchemy import create_engine

# SET PATHS -------------------------------------------------------------------

conn_str = 'CREDENTIALS_TO_ACCESS_DB'
engine = create_engine(conn_str)
connection = engine.connect()

# SQL Query to retrieve User Job Offers dataset--------------------------------

query = """

    SELECT
    vw.created_at AS date,
    jo.id AS offer_id,
    vw.user_id AS user,
    CASE WHEN ujo.job_offer_id = vw.job_offer_id 
         AND ujo.user_id = vw.user_id
         THEN TRUE ELSE FALSE
         END AS clicked,
    pt.name AS platform,
    ct.name AS city,
    cnt.name AS country

    FROM views AS vw
    LEFT JOIN job_offers AS jo
        ON vw.job_offer_id = jo.id
    JOIN user_job_offers AS ujo 
        ON ujo.job_offer_id = jo.id
    JOIN partners AS pt
        ON jo.partner_id = pt.id
    JOIN cities AS ct
        ON jo.city_id = ct.id
    JOIN countries AS cnt
        ON ct.country_id = cnt.id
    
"""

    
# LOAD DATA --------------------------------------------------------------------

jobs_raw = pd.read_sql(query, connection)

# TRANSFORM --------------------------------------------------------------------

# start by changing datetime to date to reduce granularity
jobs_raw['year_week'] = pd.to_datetime(jobs_raw['date']).dt.strftime('%Y-%U')

# PERFORMANCE ------------------------------------------------------------------

# compute columns of unique clickers, total clicks and loyalty score

clicks = jobs_raw.groupby(['year_week',
                           'country',
                           'platform',
                           'user',
                           'offer_id'])['clicked'].sum().reset_index()
    
clicks = clicks.groupby(['year_week',
                         'country',
                         'platform',
                         'offer_id'])\
    ['clicked'].agg([('unique_clickers', 
                      lambda x: (x[x != 0]).count()),
                     ('total_clicks', 'sum')])

loyalty = (clicks['total_clicks']-clicks['unique_clickers'])/clicks['total_clicks']
loyalty = loyalty.fillna(0)

# create aggregation function  
def performance(data):
    """ Calculates performance metrics for Appjobs job offer

    The function aggregates the jobs_raw table

    Parameters
    ----------
    data : pandas DataFrame
        Name of the DataFrame that will be used for the calculation.
    Returns
    -------
    an aggregated pandas DataFrame 
    """
    unique_viewers = data.user.nunique()
    total_views = len(data.index)
    attractiveness = (total_views - unique_viewers) / total_views
    ctr = data.clicked.sum() / data.clicked.count()
    return pd.Series({'unique_viewers': unique_viewers,
                      'total_views': total_views,
                      'attractiveness': attractiveness,
                      'ctr': ctr})

jobs_group = jobs_raw.groupby(['year_week',
                                 'country',
                                 'platform',
                                 'offer_id']).apply(performance)

# merging all performance scores
jobs_group = jobs_group.merge(clicks,
                               left_index=True,
                               right_index=True)
jobs_group = jobs_group.merge(loyalty.rename('loyalty'),
                               left_index=True,
                               right_index=True)
jobs_agg = jobs_group.reset_index()

# Create platform average performance with respect to job offers
jobs_avg = jobs_agg.groupby(['year_week',
                             'country',
                             'platform']).mean().drop(columns='offer_id').round(2)


jobs_avg = jobs_avg.reset_index()
# Remove all job offers that are to unique in there characteristics
job_offers_anonymized = jobs_avg[jobs_avg['unique_clickers'] > 20].reset_index()
job_offers_anonymized = job_offers_anonymized.drop(columns='index')


# PUSH TABLE TO DB -------------------------------------------------------------

job_offers_anonymized.to_sql('job_offers', 
                             connection, schema='dev', if_exists='replace')

connection.close()
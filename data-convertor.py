import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import unicodedata as ud

engine = create_engine("mysql://USER:PASSWORD@HOST/DATABASE?charset=utf8mb4")
conn = engine.connect()


c_size = 100_000

for data_chunk in pd.read_csv('../data3.tsv', sep='\t', chunksize=c_size, error_bad_lines=False, encoding='utf8'):
    customres = data_chunk[['customer_id']].drop_duplicates().copy()
    products = data_chunk[['marketplace','product_id','product_parent','product_title','product_category']].drop_duplicates(subset=['product_id']).copy()

    try:
        products.to_sql('products', conn, if_exists = 'append', index = False)
        print('products loaded')
    except sa.exc.IntegrityError:
        pass
    except sa.exc.OperationalError:
        pass

    try:
        customres.to_sql('customers', conn, if_exists = 'append', index = False)
        print('customers loaded')
    except sa.exc.IntegrityError:
        pass
    except sa.exc.OperationalError:
        pass

    print('Chunk loaded p+c')

c_size = 100_000

for data_chunk in pd.read_csv('../data3.tsv', sep='\t', chunksize=c_size, error_bad_lines=False, encoding='utf8'):
    reviews = data_chunk[['customer_id', 'review_id', 'product_id', 'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase', 'review_headline', 'review_body', 'review_date']].drop_duplicates(subset=['review_id']).copy()

    try:
        reviews.to_sql('reviews', conn, if_exists = 'append', index = False)
        print('reviews loaded')
    except sa.exc.IntegrityError:
        pass
    except sa.exc.OperationalError:
        pass

    print('Chunk loaded r')

conn.close()
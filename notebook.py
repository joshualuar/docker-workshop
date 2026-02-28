#!/usr/bin/env python
# coding: utf-8

# In[76]:


import pandas as pd

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz') #, nrows=100



# In[77]:


# Display first rows
df.head()



# In[78]:


len(df)


# In[79]:


# Check data types
df.dtypes


# In[80]:


# Check data shape
df.shape


# In[81]:


df['tpep_pickup_datetime']


# In[82]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[83]:


get_ipython().system('uv add sqlalchemy "psycopg[binary,pool]"')


# ## Pasar la data posgres y lo pueda ver en el docker

# In[84]:


from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')


# In[85]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[86]:


#df.to_sql(name='yellow_taxi_data', con=engine)
## la cambié para poder sobreescribir
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace', index=False)


# In[87]:


#Crear esquema


# In[88]:


df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[89]:


#Partir la db en pedazos


# In[90]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[91]:


#Para ver los pedazos


# In[92]:


df = next(df_iter)


# In[93]:


df


# In[94]:


#PAra ver cada elemento

for df_chunk in df_iter:
    df_chunk.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
print(len(df))

# In[95]:


get_ipython().system('uv add tqdm')


# In[96]:


from tqdm.auto import tqdm


# In[101]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[100]:


len(df_chunk)


# In[ ]:





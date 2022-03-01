#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark


# In[2]:


pyspark.__file__


# In[3]:


from pyspark.sql import SparkSession


# In[4]:



spark = SparkSession.builder     .master("local[*]")     .appName('test')     .getOrCreate()


# In[5]:


spark.version


# In[11]:


get_ipython().system('wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv')


# In[29]:


get_ipython().system('head -n 1001 fhvhv_tripdata_2021-02.csv > head.csv')


# In[9]:


import pandas as pd


# In[30]:


df_pandas = pd.read_csv('head.csv')


# In[16]:


spark.createDataFrame(df_pandas).schema


# In[18]:


from pyspark.sql import types


# In[19]:


schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True)
])


# In[20]:


df = spark.read     .option("header", "true")     .schema(schema)     .csv('fhvhv_tripdata_2021-02.csv')


# In[21]:


df = df.repartition(24)


# In[22]:


df.write.parquet('fhvhv/2021/02/')


# In[31]:


df_pandas


# In[45]:



from pyspark.sql.functions import *

df = df.withColumn("datecol",to_date("pickup_datetime")) 


# In[46]:


df.schema


# In[49]:


df.where(df.datecol == '2021-02-15').count()


# In[78]:


df = df.withColumn('duration',unix_timestamp("dropoff_datetime").cast("long") - unix_timestamp('pickup_datetime').cast("long"))


# In[79]:


df.schema


# In[80]:


dff = df.orderBy('duration', ascending=False)


# In[81]:


dff.show(1)


# In[95]:


df_zones1 = spark.read     .option("header", "true")     .csv('taxi+_zone_lookup.csv')


# In[96]:


df_zones2 = spark.read     .option("header", "true")     .csv('taxi+_zone_lookup.csv')


# In[83]:


df.registerTempTable('fhv')
df_zones.registerTempTable('zone')


# In[86]:


dff = spark.sql("""

select concat(coalesce(z1.Zone,'Unknown'),'/',coalesce(z2.Zone,'Unknown')),COUNT(*)
from zone z1 
inner join fhv a 
on z1.LocationID=a.PULocationID 
inner join zone z2 
on z2.LocationID=a.DOLocationID
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
""")


# In[ ]:





# In[88]:


dff.show(truncate=False)


# In[90]:


dff = spark.sql("""

select a.dispatching_base_num,COUNT(*)
from fhv a 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1
""")


# In[91]:


dff.show()


# In[93]:


df = df.groupby("dispatching_base_num").count().orderBy(col("count").desc())


# In[94]:


df.show(1)


# In[ ]:





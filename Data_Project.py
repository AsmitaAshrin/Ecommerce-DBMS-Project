#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import glob
import psycopg2
import pandas as pd
#from sql_queries import *


# In[2]:


pip install sql_queries


# # Creating Connection with postgreSQL

# In[3]:


host = 'localhost'
dbname = 'postgres'
user = 'postgres'
password = '12345'


# In[4]:


conn = psycopg2.connect(
    host=host,
    dbname=dbname,
    user=user,
    password=password
)


# In[5]:


cur = conn.cursor()


# # Loading Data in Pandas

# In[6]:


Fact_table = pd.read_csv('Fact_data.csv')
Customer_dim = pd.read_csv('Coustomer_dim.csv')
Item_dim = pd.read_csv('Item_dim.csv')
Store_dim = pd.read_csv('Store_dim.csv')
Time_dim = pd.read_csv('Time_dim.csv')
Trans_dim = pd.read_csv('Trans_dim.csv')


# In[26]:


Store_dim


# # Creating Table in Postgres

# In[8]:


cur = conn.cursor()


# cur.execute('''CREATE TABLE ecomdb.Customer_dim (
#                     coustomer_key VARCHAR(10) PRIMARY KEY NOT NULL,
#                     name VARCHAR(50),
#                     contact_no INT,
#                     nid INT
#                 );''')

# In[35]:


cur.execute("CREATE TABLE ecomdb.Customer_dim (coustomer_key VARCHAR(10) PRIMARY KEY NOT NULL, name VARCHAR(50), contact_no INT, nid INT)")
conn.commit()


# In[38]:


cur.execute("CREATE TABLE ecomdb.Item_dim (item_key VARCHAR(50) PRIMARY KEY NOT NULL, item_name VARCHAR(50), \"desc\" VARCHAR(10), unit_price INT, man_country VARCHAR(50), supplier VARCHAR(50), unit VARCHAR(50))")


# In[ ]:


conn.commit()


# In[39]:


cur.execute("CREATE TABLE ecomdb.Store_dim (store_key VARCHAR(50) PRIMARY KEY NOT NULL, division CHAR(50), district CHAR(50), upazila CHAR(50))")


# In[ ]:


conn.commit()


# In[ ]:


cur.execute("CREATE TABLE ecomdb.Time_dim (time_key VARCHAR(50) PRIMARY KEY NOT NULL, date DATE, hour INT, day INT,week VARCHAR(50),month INT,quarter VARCHAR(5),year VARCHAR(5))")
conn.commit()


# In[ ]:


cur.execute("CREATE TABLE ecomdb.Trans_dim (payment_key VARCHAR(10) PRIMARY KEY NOT NULL, trans_type CHAR(10), bank_name CHAR(50))")
conn.commit()


# In[ ]:


cur.execute("""
CREATE TABLE ecomdb.Fact_table (
    payment_key VARCHAR(50) NOT NULL,
    coustomer_key VARCHAR(50) NOT NULL,
    time_key VARCHAR(50) NOT NULL,
    item_key VARCHAR(50) NOT NULL,
    store_key VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL,
    CONSTRAINT Fact_table_trans_fk FOREIGN KEY (payment_key) REFERENCES ecomdb.Trans_dim (payment_key) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT Fact_table_customer_fk FOREIGN KEY (coustomer_key) REFERENCES ecomdb.Customer_dim (coustomer_key) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT Fact_table_time_fk FOREIGN KEY (time_key) REFERENCES ecomdb.Time_dim (time_key) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT Fact_table_item_fk FOREIGN KEY (item_key) REFERENCES ecomdb.Item_dim (item_key) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT Fact_table_store_fk FOREIGN KEY (store_key) REFERENCES ecomdb.Store_dim (store_key) ON DELETE CASCADE ON UPDATE CASCADE
);
""")


# In[ ]:


conn.rollback()


# In[ ]:


conn.commit()


# In[ ]:


Customer_dim.to_sql('ecomdb.Customer_dim', conn, if_exists='append', index=False)


# In[30]:


for i, row in Customer_dim.iterrows():
    cur.execute(
        "INSERT INTO ecomdb.Customer_dim (coustomer_key, name, contact_no, nid) VALUES (%s, %s, %s, %s)",
        (row['coustomer_key'], row['name'], row['contact_no'], row['nid'])
    )
    conn.commit()


# In[16]:


conn.rollback()


# In[52]:


for i, row in Item_dim.iterrows():
    cur.execute(
        "INSERT INTO ecomdb.Item_dim (item_key, item_name, \"desc\", unit_price, man_country, supplier, unit) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (row['item_key'], row['item_name'], row['desc'], row['unit_price'], row['man_country'], row['supplier'], row['unit'])
    )
    conn.commit()


# In[33]:


len Customer_dim


# In[54]:


for i, row in Store_dim.iterrows():
    cur.execute(
        "INSERT INTO ecomdb.Store_dim (store_key, division, district, upazila) VALUES (%s, %s, %s, %s)",
        (row['store_key'], row['division'], row['district'], row['upazila'])
    )
    conn.commit()


# In[34]:


len (Customer_dim)


# In[64]:


for i, row in Time_dim.iterrows():
    cur.execute(
        "INSERT INTO ecomdb.Time_dim (time_key, date, hour, day, week, month, quarter, year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (row['time_key'], row['date'], row['hour'], row['day'], row['week'], row['month'], row['quarter'], row['year'])
    )
    conn.commit()


# In[11]:


for i, row in Trans_dim.iterrows():
    cur.execute(
        "INSERT INTO ecomdb.Trans_dim (payment_key, trans_type, bank_name) VALUES (%s, %s, %s)",
        (row['payment_key'], row['trans_type'], row['bank_name'])
    )
    conn.commit()


# In[14]:


for i, row in Fact_table.iterrows():
    cur.execute(
        "INSERT INTO ecomdb.Fact_table (payment_key, coustomer_key, time_key, item_key, store_key, quantity, unit, unit_price, total_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row['payment_key'], row['coustomer_key'], row['time_key'], row['item_key'], row['store_key'], row['quantity'],row['unit'], row['unit_price'], row['total_price'])
    )
    conn.commit()


# # Data Analytics

# In[ ]:





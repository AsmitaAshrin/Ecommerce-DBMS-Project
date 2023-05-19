#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install matplotlib


# In[2]:


import os
import glob
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


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


# In[6]:


Fact_table = pd.read_csv('Fact_data.csv')
Customer_dim = pd.read_csv('Coustomer_dim.csv')
Item_dim = pd.read_csv('Item_dim.csv')
Store_dim = pd.read_csv('Store_dim.csv')
Time_dim = pd.read_csv('Time_dim.csv')
Trans_dim = pd.read_csv('Trans_dim.csv')


# In[7]:


conn.rollback()


# # Analytics

# Q1

# In[8]:


que_month_sales = ("""Select distinct(month), sum(total_price)Total_Sales
               From ecomdb.Time_dim as t, ecomdb.Fact_table as f
               Where t.time_key= f.time_key
               Group by month
               Order by sum(total_price) desc""")


# In[9]:


month_wise_sales = pd.read_sql(que_month_sales, conn)


# In[10]:


month_wise_sales


# In[11]:


que_division_sales = ("""Select distinct(division), sum(total_price)Total_Sales
               From ecomdb.Store_dim as t, ecomdb.Fact_table as f
               Where t.store_key= f.store_key
               Group by division
               Order by sum(total_price) desc""")
division_wise_sales = pd.read_sql(que_division_sales, conn)
division_wise_sales


# In[12]:


que_district_sales = ("""Select distinct(district), sum(total_price)Total_Sales
               From ecomdb.Store_dim as t, ecomdb.Fact_table as f
               Where t.store_key= f.store_key
               Group by district
               Order by sum(total_price)desc""")
district_wise_sales = pd.read_sql(que_district_sales, conn)
district_wise_sales


# In[13]:


que_year_sales = ("""Select distinct(year), sum(total_price)Total_Sales
               From ecomdb.Time_dim as t, ecomdb.Fact_table as f
               Where t.time_key= f.time_key
               Group by year
               Order by sum(total_price)desc""")
year_wise_sales = pd.read_sql(que_year_sales, conn)
year_wise_sales


# Q2

# In[14]:


que_customer_sales = ("""Select distinct(name)Customer, sum(total_price)Total_Sales
               From ecomdb.Customer_dim as t, ecomdb.Fact_table as f
               Where t.coustomer_key= f.coustomer_key
               Group by name
               Order by sum(total_price)desc""")
cutomer_wise_sales = pd.read_sql(que_customer_sales, conn)
cutomer_wise_sales


# In[15]:


que_bank_sales = ("""Select distinct(bank_name), sum(total_price)Total_Sales
               From ecomdb.Trans_dim as t, ecomdb.Fact_table as f
               Where t.payment_key= f.payment_key
               Group by bank_name
               Order by sum(total_price)desc""")
bank_wise_sales = pd.read_sql(que_bank_sales, conn)
bank_wise_sales


# In[16]:


que_trans_type_sales = ("""Select distinct(trans_type), sum(total_price)Total_Sales
               From ecomdb.Trans_dim as t, ecomdb.Fact_table as f
               Where t.payment_key= f.payment_key
               Group by trans_type
               Order by sum(total_price)desc""")
bank_trans_type_sales = pd.read_sql(que_trans_type_sales, conn)
bank_trans_type_sales


# Q3

# In[17]:


que_Barisal_Pepsi_sales = ("""Select district, sum(total_price)Pepsi_Sales
               From ecomdb.Store_dim as t, ecomdb.Fact_table as f, ecomdb.Item_dim as i
               Where t.store_key= f.store_key 
               and district = 'BARISAL' 
               and f.item_key in (Select item_key From ecomdb.Item_dim Where item_name = 'Pepsi - 12 oz cans')
               Group by district
               Order by sum(total_price)desc""")
Barisal_Pepsi_sales = pd.read_sql(que_Barisal_Pepsi_sales, conn)
Barisal_Pepsi_sales


# Q4

# In[18]:


que_Bigso_Supply_sales = ("""Select year, sum(total_price)Bigso_Supplier_Sales_2015
               From ecomdb.Time_dim as t, ecomdb.Fact_table as f, ecomdb.Item_dim as i
               Where t.time_key= f.time_key and year = '2015' and f.item_key in (Select item_key From ecomdb.Item_dim Where supplier = 'BIGSO AB')
               Group by year
               Order by sum(total_price)desc""")
Bigso_Supply_sales = pd.read_sql(que_Bigso_Supply_sales, conn)
Bigso_Supply_sales


# Q5

# In[19]:


que_Dhaka_Sales_2015 = ("""Select i.division, sum(total_price)Sales_2015
From ecomdb.Time_dim as t, ecomdb.Fact_table as f, ecomdb.Store_dim as i
Where t.time_key= f.time_key and year = '2015' and i.store_key = f.store_key and i.store_key in (Select store_key FROM ecomdb.Store_dim WHERE division = 'DHAKA' )
Group by i.division
Order by sum(total_price) desc
""")
Dhaka_Sales_2015 = pd.read_sql(que_Dhaka_Sales_2015, conn)
Dhaka_Sales_2015


# Q6

# In[20]:


que_Top_3_products = ("""Select i.item_name, sum(quantity)Quantity_Sales
From ecomdb.Fact_table as f, ecomdb.Item_dim as i
Where i.item_key = f.item_key 
Group by i.item_name
Order by sum(quantity) desc
""")
Top_3_products = pd.read_sql(que_Top_3_products, conn)
Top_3_products.head(3)


# Q7

# In[53]:


n = int(input("Since days: ")) - 1
que_prod_since_x_day = """

WITH max_date AS (
  SELECT MAX(to_date(date, 'DD-MM-YYYY HH24:MI')) AS max_date
  FROM ecomdb.Time_dim
), last_n_days AS (
  SELECT (max_date - INTERVAL '1 day'*(5-1)) AS start_date, max_date AS end_date
  FROM max_date
)
SELECT DISTINCT i.item_name AS product_name
FROM ecomdb.Fact_table AS f
JOIN ecomdb.Trans_dim AS t ON f.payment_key = t.payment_key
JOIN ecomdb.Item_dim AS i ON f.item_key = i.item_key
JOIN ecomdb.Time_dim AS td ON f.time_key = td.time_key
CROSS JOIN last_n_days
WHERE t.trans_type IN ('card', 'mobile')
AND to_date(td.date, 'DD-MM-YYYY HH24:MI') BETWEEN start_date AND end_date
"""
prod_since_x_day = pd.read_sql(que_prod_since_x_day, conn, params=(n,))
prod_since_x_day


# Q8

# In[21]:


que_worst_prod_quarter= ("""SELECT item_name, quarter, SUM(quantity) AS total_sales
FROM ecomdb.Time_dim
JOIN ecomdb.Fact_table ON ecomdb.Time_dim.time_key = ecomdb.Fact_table.time_key
JOIN ecomdb.Item_dim ON ecomdb.Fact_table.item_key = ecomdb.Item_dim.item_key
GROUP BY item_name, quarter
HAVING SUM(quantity) = (
    SELECT MIN(sales.total_sales)
    FROM (
        SELECT item_name, quarter, SUM(quantity) AS total_sales
        FROM ecomdb.Time_dim
        JOIN ecomdb.Fact_table ON ecomdb.Time_dim.time_key = ecomdb.Fact_table.time_key
        JOIN ecomdb.Item_dim ON ecomdb.Fact_table.item_key = ecomdb.Item_dim.item_key
        GROUP BY item_name, quarter
    ) AS sales
    WHERE sales.item_name = ecomdb.Item_dim.item_name
)
ORDER BY item_name
""")
worst_product_for_Q = pd.read_sql(que_worst_prod_quarter, conn)
worst_product_for_Q


# Q9

# In[22]:


que_item_division_sale = ("""SELECT i.item_name, s.division, SUM(f.total_price) AS total_sales
FROM ecomdb.Fact_table AS f
JOIN ecomdb.Item_dim AS i ON f.item_key = i.item_key
JOIN ecomdb.Store_dim AS s ON f.store_key = s.store_key
GROUP BY i.item_name, s.division
ORDER BY i.item_name ASC, s.division ASC;
""")
item_division_sale = pd.read_sql(que_item_division_sale, conn)
item_division_sale


# Q10

# In[23]:


que_item_month_sale = ("""SELECT i.item_name, t.month, AVG(f.total_price) AS avg_sales
FROM ecomdb.Item_dim AS i, ecomdb.Fact_table AS f, ecomdb.Time_dim AS t, ecomdb.Store_dim AS s
WHERE i.item_key = f.item_key
AND f.time_key = t.time_key
AND f.store_key = s.store_key
GROUP BY i.item_name, t.month
ORDER BY i.item_name, t.month""")
item_month_sale = pd.read_sql(que_item_month_sale,conn)
item_month_sale


# In[24]:


# Pivot the dataframe to make item_name the index and month the columns
df_pivot = item_month_sale.pivot(index='item_name', columns='month', values='avg_sales')

# Create the stacked bar chart
df_pivot.plot(kind='bar', stacked=True)

# Add labels and title
plt.xlabel('Item')
plt.ylabel('Average Sales')
plt.title('Average Sales of Top 10 Items per Month')

# Show the chart
plt.show()


# In[36]:


pip install statsmodels


# In[33]:


pip install sklearn


# Task 4 (5)

# In[42]:


que_category_avg_monthly_trend = ("""SELECT i.desc AS category, t.month, SUM(f.total_price) AS monthly_sales
FROM ecomdb.Item_dim AS i
JOIN ecomdb.Fact_table AS f ON i.item_key = f.item_key
JOIN ecomdb.Time_dim AS t ON f.time_key = t.time_key
GROUP BY i.desc, t.month
ORDER BY i.desc, t.month;""")
category_avg_monthly_trend = pd.read_sql(que_category_avg_monthly_trend,conn)
category_avg_monthly_trend


# In[46]:


que_category_avg_monthly_best_trend = ("""SELECT t.category, t.month, t.monthly_sales
FROM (
  SELECT i.desc AS category, t.month, SUM(f.total_price) AS monthly_sales,
         RANK() OVER (PARTITION BY i.desc ORDER BY SUM(f.total_price) DESC) AS sales_rank
  FROM ecomdb.Item_dim AS i
  JOIN ecomdb.Fact_table AS f ON i.item_key = f.item_key
  JOIN ecomdb.Time_dim AS t ON f.time_key = t.time_key
  GROUP BY i.desc, t.month
) t
WHERE t.sales_rank = 1
ORDER BY month;""")
category_avg_monthly_best_trend = pd.read_sql(que_category_avg_monthly_best_trend,conn)
category_avg_monthly_best_trend.head(15)


# In[ ]:





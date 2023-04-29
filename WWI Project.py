#!/usr/bin/env python
# coding: utf-8

# Wide World Importers Data Analytics with PySpark

# DESCRIPTION:

# Wide World Importers (WWI) is a goods importer and distributor operating from the San Francisco.
# 
# As a wholesaler, WWI's customers are mostly companies who resell to individuals. WWI sells to retail customers across the United States including specialty stores, supermarkets, computing stores, tourist attraction shops, and some individuals. WWI also sells to other wholesalers via a network of agents who promote the products on WWI's behalf. While all of WWI's customers are currently based in the United States, the company is intending to push for expansion into other countries/regions.
# 
# WWI buys goods from suppliers including novelty and toy manufacturers, and other novelty wholesalers. They stock the goods in their WWI warehouse and reorder from suppliers as needed to fulfill customer orders. They also purchase large volumes of packaging materials, and sell these in smaller quantities as a convenience for the customers.

# SOURCE:

# WWI dataset that is used in this project was downloaded from DataCamp source.

# IMPORTING LIBRARIES AND STARTING SPARK SESSION:

# In[2]:


get_ipython().system('pip install pyspark')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

from pyspark.sql.functions import col,isnan,when,count
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("WWI_Analysis").getOrCreate()


# KOALAS LIBRARY:

# Koalas supports Apache Spark 3.1 and below as it is officially included to PySpark in Apache Spark 3.2 and above.
# Spark version 3.3.2 works here, so in this project PySpark will be used directly.

# In[4]:


spark.version


# READING DATA FILES:

# FACT INTERNET SALE TABLE
# 
# The main table in the WWI tabel hirarhy, contains important information about the actual sales of products. It has 8 keys that link this table with others. All data types are correct.

# In[6]:


fact_internet_sale = spark.read.option('header', 'true').csv('Tables_WWI/FactInternetSale.csv', inferSchema=True)
fact_internet_sale.printSchema()


# In[7]:


rows = fact_internet_sale.count()
print(f"fact_internet_sale DataFrame Rows count : {rows}")


# Extract the required data from the Fact Internet Sale table using the spark.sql file, changing some of the names to make them clearer:

# In[80]:


fact_internet_sale.createOrReplaceTempView('FactInternetSale')
fact_int_sale_sql = spark.sql('SELECT ProductKey, OrderDateKey, SalesTerritoryKey, OrderQuantity AS UnitsSold, TotalProductCost, SalesAmount AS Revenue, TaxAmt AS Taxes, Freight AS DeliveryCost FROM FactInternetSale')
fact_int_sale_sql.show(5)


# Creating "Profit" column: 

# In[89]:


profit_sale_sql = fact_int_sale_sql.withColumn("Profit", fact_int_sale_sql.Revenue - (fact_int_sale_sql.TotalProductCost + fact_int_sale_sql.Taxes))
profit_sale_sql.show(3)


# DATE TABLE
# 
# Contains day, month and year data linked to the FactSale table, which explains the time and date of the sale.
# Includes two Key columns, a large number of duplicate columns for the names of days of the week and months in three languages: English, French and Spanish. All the data types are correct.

# In[10]:


date = spark.read.option('header', 'true').csv('Tables_WWI/DimDate.csv', inferSchema=True)
date.printSchema()


# The total number of days recorded: 4017

# In[11]:


rows = date.count()
print(f"date DataFrame Rows count : {rows}")


# Extract the required data from the Date table using spark.sql:

# In[12]:


date.createOrReplaceTempView('Date')
date_sql = spark.sql('SELECT DateKey, FullDateAlternateKey, DayNumberOfYear, EnglishMonthName, CalendarYear FROM date')
date_sql.show(5)


# Below we can see that the data record is from 2005-01-01 to 2015-12-31:

# In[13]:


date_sql.select(min(date_sql.FullDateAlternateKey).alias("FirstDate"), 
          max(date_sql.FullDateAlternateKey).alias("LastDate")
    ).show()


# SALES TERRITORY TABLE
# 
# Also a small table containing information on the places where products are sold. It has two keys and contains 10 different countries.
# All data types are correct.

# In[14]:


sales_territory = spark.read.option('header', 'true').csv('Tables_WWI/DimSalesTerritory.csv', inferSchema=True)
sales_territory.printSchema()


# In[16]:


rows = sales_territory.count()
print(f"sales_territory DataFrame Rows count : {rows}")


# In[17]:


sales_territory.createOrReplaceTempView('SalesTerritory')
sales_territory_sql = spark.sql('SELECT SalesTerritoryKey, SalesTerritoryCountry AS Country, SalesTerritoryRegion AS Region FROM SalesTerritory')
sales_territory_sql.show()


# PRODUCT TABLE
# 
# Describes the type, class, style and other parameters of the product.
# Includes three Key columns, a large number of duplicate columns for ProductName in three languages: English, French and Spanish. Description dublicates in English, French, Chinese, Arabic, Hebrew, Thai, German, Japanese and Turkish.
# All the data types are correct.

# In[3]:


product = spark.read.option('header', 'true').csv('Tables_WWI/DimProduct.csv', inferSchema=True)
product.printSchema()


# In[20]:


rows = product.count()
print(f"product DataFrame Rows count : {rows}")


# Extract the required data from the Product table using spark.sql:

# In[4]:


product.createOrReplaceTempView('Product')
product_sql = spark.sql('SELECT ProductKey, ProductSubcategoryKey, EnglishProductName AS ProductName FROM Product')
product_sql.show(5)


# ProductSubcategoryKey values need to be converted from string to integer. This makes it possible to create a relation between the product and product_subcategory tables by the ProductSubcategoryKey. 

# In[5]:


product_sql = product_sql.withColumn("ProductSubcategoryKey", product_sql["ProductSubcategoryKey"].cast(IntegerType()))


# In[6]:


product_sql.printSchema()


# Checking for NULL values of ProductSubcategoryKey column.

# In[23]:


spark.sql('SELECT COUNT(ProductKey) FROM Product WHERE ProductSubcategoryKey IS NULL').show()


# PRODUCT SUBCATEGORY TABLE
# 
# A small table containing information about a sub-category of a product. Has two Keys and contains 37 different types of sub-categories.
# All the data types are correct.

# In[24]:


product_subcategory = spark.read.option('header', 'true').csv('Tables_WWI/DimProductSubcategory.csv', inferSchema=True)
product_subcategory.printSchema()


# In[26]:


rows = product_subcategory.count()
print(f"product_subcategory DataFrame Rows count : {rows}")


# In[27]:


product_subcategory.createOrReplaceTempView('ProductSubcategory')
product_subcategory_sql = spark.sql('SELECT ProductSubcategoryKey, ProductCategoryKey, EnglishProductSubcategoryName AS SubCategory FROM ProductSubcategory')
product_subcategory_sql.show(5)


# Checking for unique SubCategory values:

# In[29]:


dist = product_subcategory_sql.select(countDistinct("SubCategory"))
dist.show()


# PRODUCT CATEGORY TABLE
# 
# Similar to product subcategory table. Contains important information about the main categories of products. It has two keys and all data types are correct.

# In[30]:


product_category = spark.read.option('header', 'true').csv('Tables_WWI/ProductCategory.csv', inferSchema=True)
product_category.printSchema()


# Below we see that there are 4 unique types of product categories:

# In[31]:


product_category.createOrReplaceTempView('ProductCategory')
product_category_sql = spark.sql('SELECT ProductCategoryKey, EnglishProductCategoryName AS Category FROM ProductCategory')
product_category_sql.show()


# JOINING TABLES

# Firstly, it was decided to combine all the tables relating to product features, such as product_subcategory_sql, product_category_sql and finally product_sql. 

# In[32]:


category_table = product_subcategory_sql.join(product_category_sql, ['ProductCategoryKey'], 'inner')
category_table.show(3)


# In[33]:


product_table = product_sql.join(category_table, ['ProductSubcategoryKey'], 'inner')
product_table.show(3)


# Afterwards profit_sale_sql, date_sql, sales_territory_sql and product_table were joined.

# In[86]:


analytical_table = profit_sale_sql.join(date_sql).where(profit_sale_sql['OrderDateKey'] == date_sql['DateKey']).join(sales_territory_sql, ['SalesTerritoryKey']).join(product_table, ['ProductKey'])
analytical_table.show(3)


# In[87]:


analytical_table.printSchema()


# The new table is an analytical base table (ABT) that is used for data analysis and visualisation, it has the same number of rows as in main table fact_internet_sale:

# In[43]:


rows = analytical_table.count()
print(f"analytical_table DataFrame Rows count : {rows}")


# SUM OF UnitsSold, Revenue AND Profit:

# In[97]:


analytical_table.createOrReplaceTempView('AnalyticalTable')
spark.sql('SELECT SUM(UnitsSold), SUM(Revenue), ROUND(SUM(Profit), 2) FROM AnalyticalTable ORDER BY SUM(UnitsSold) DESC, SUM(Revenue) DESC, SUM(Profit) DESC').show()


# UnitsSold AND Profit BY COUNTRY:

# In[72]:


spark.sql('SELECT Country, COUNT(UnitsSold), ROUND(SUM(Profit), 2) FROM AnalyticalTable GROUP BY Country ORDER BY COUNT(UnitsSold) DESC, SUM(Profit) DESC').show()


# In[ ]:


PROFIT BY Category:


# In[71]:


spark.sql('SELECT Category, ROUND(SUM(Profit), 2) FROM AnalyticalTable GROUP BY Category ORDER BY SUM(Profit) DESC').show()


# In[ ]:


PROFIT BY SubCategory:


# In[69]:


spark.sql('SELECT SubCategory, ROUND(SUM(Profit), 2) FROM AnalyticalTable GROUP BY SubCategory ORDER BY SUM(Profit) DESC').show()


# SUM OF TotalProductCost, Revenue, Profit BY CalendarYear:

# In[65]:


spark.sql('SELECT CalendarYear, SUM(TotalProductCost), SUM(Revenue), SUM(Profit) FROM AnalyticalTable GROUP BY CalendarYear ORDER BY CalendarYear DESC').show()


# SAVE CSV FILE:

# In[25]:


analytic_table.coalesce(1).write.csv('Tables_WWI/AnalyticTable.csv', mode='overwrite', header=True)


# CONCLUSIONS:

# - The Wide World Importers Dataset takes 20.4 MB and consist of 6 tabs. 
# - It was discovered, that the main table is FactSales.csv, that stores facts related to a business operations, it is located at the center of a schema and is supplemented by the dimension tables: DimCustomer.csv, DimEmployee.xlxs, DimStockItem.csv, DimDate.csv and DimCity.csv.
# - Data architecture has been studied: the dataset structure is a Snowflake - a multi-dimensional data model that is similar to a star schema, but where dimension tables are broken down into subdimensions? as it is in all the tables relating to product features (product_subcategory_sql, product_category_sql and product_sql).
# - Each WWI table has between 2-8 Keys, which ensures linking between the tables, appropriate use and storage of the data.
# - Was discovered that 60 000 units were sold, total sum of revenue is 29.36M dollars and total profit is 9.73M.
# - The biggest profit is in 2014 year - 3268022.5 despite the fact that there is data only from January to start of July. When it comes to category the biggest profit is from Bikes - 95%.
# - The data visualisation is made with PowerBI and presented as a pdf file.

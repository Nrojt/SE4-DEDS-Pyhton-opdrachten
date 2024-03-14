#!/usr/bin/env python


import pandas as pd
import pyodbc
import sqlite3
import datetime
import numpy as np


DB = {
    'servername': '(local)\\SQLEXPRESS',
    'database': 'DEDS_DataWarehouse',}

export_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')

export_cursor = export_conn.cursor()

# check if connection is successful, else throw an error
if export_conn:
    print("Connection with database is established")
else:
    print("Connection with database is not established")
    #raise Exception("Connection with database is not established")


# # Brontabellen
# importeer de brontabellen uit de data foler


# Connectie met sqlite databases
go_crm_connection = sqlite3.connect('data/raw/go_crm.sqlite')
go_sales_connection = sqlite3.connect('data/raw/go_sales.sqlite')
go_staff_connection = sqlite3.connect('data/raw/go_staff.sqlite')


# inlezen csv bestanden
go_sales_inventory_levels = pd.read_csv('data/raw/GO_SALES_INVENTORY_LEVELSData.csv')
go_sales_product_forcast = pd.read_csv('data/raw/GO_SALES_PRODUCT_FORECASTData.csv')


# ## Inlezen van de sqlite tabellen

go_crm_age_group = pd.read_sql_query("SELECT * FROM age_group", go_crm_connection)
go_crm_country = pd.read_sql_query("SELECT * FROM country", go_crm_connection)
go_crm_retailer = pd.read_sql_query("SELECT * FROM retailer", go_crm_connection)
go_crm_retailer_contact = pd.read_sql_query("SELECT * FROM retailer_contact", go_crm_connection)
go_crm_retailer_headquarters = pd.read_sql_query("SELECT * FROM retailer_headquarters", go_crm_connection)
go_crm_retailer_segment = pd.read_sql_query("SELECT * FROM retailer_segment", go_crm_connection)
go_crm_retailer_site = pd.read_sql_query("SELECT * FROM retailer_site", go_crm_connection)
go_crm_retailer_type = pd.read_sql_query("SELECT * FROM retailer_type", go_crm_connection)
go_crm_sales_demographic = pd.read_sql_query("SELECT * FROM sales_demographic", go_crm_connection)
go_crm_sales_territory = pd.read_sql_query("SELECT * FROM sales_territory", go_crm_connection)



go_sales_country = pd.read_sql_query("SELECT * FROM country", go_sales_connection)
go_sales_order_details = pd.read_sql_query("SELECT * FROM order_details", go_sales_connection)
go_sales_order_header = pd.read_sql_query("SELECT * FROM order_header", go_sales_connection)
go_sales_order_method = pd.read_sql_query("SELECT * FROM order_method", go_sales_connection)
go_sales_product = pd.read_sql_query("SELECT * FROM product", go_sales_connection)
go_sales_product_line = pd.read_sql_query("SELECT * FROM product_line", go_sales_connection)
go_sales_product_type = pd.read_sql_query("SELECT * FROM product_type", go_sales_connection)
go_sales_retailer_site = pd.read_sql_query("SELECT * FROM retailer_site", go_sales_connection)
go_sales_return_reason = pd.read_sql_query("SELECT * FROM return_reason", go_sales_connection)
go_sales_returned_item = pd.read_sql_query("SELECT * FROM returned_item", go_sales_connection)
go_sales_sales_branch = pd.read_sql_query("SELECT * FROM sales_branch", go_sales_connection)
go_sales_sales_staff = pd.read_sql_query("SELECT * FROM sales_staff", go_sales_connection)
go_sales_sales_target_data = pd.read_sql("SELECT * FROM SALES_TARGETData", go_sales_connection)



go_staff_course = pd.read_sql_query("SELECT * FROM course", go_staff_connection)
go_staff_sales_branch = pd.read_sql_query("SELECT * FROM sales_branch", go_staff_connection)
go_staff_sales_staff = pd.read_sql_query("SELECT * FROM sales_staff", go_staff_connection)
go_staff_satisfaction = pd.read_sql_query("SELECT * FROM satisfaction", go_staff_connection)
go_staff_satisfaction_type = pd.read_sql_query("SELECT * FROM satisfaction_type", go_staff_connection)
go_staff_training = pd.read_sql_query("SELECT * FROM training", go_staff_connection)


all_dataframes = []


# to be used in order details
returned_item_columns = ['RETURN_CODE', 'RETURN_DATE', 'RETURN_QUANTITY', 'ORDER_DETAIL_CODE', 'RETURN_REASON_CODE', 'RETURN_DESCRIPTION_EN']
returned_item = pd.merge(go_sales_returned_item, go_sales_return_reason, left_on='RETURN_REASON_CODE', right_on='RETURN_REASON_CODE')
returned_item = pd.merge(returned_item, go_sales_order_details, left_on='ORDER_DETAIL_CODE', right_on='ORDER_DETAIL_CODE')

# filtering the columns
returned_item = returned_item[returned_item_columns]

# renaming the columns
returned_item = returned_item.rename(columns={'RETURN_CODE': 'RETURNED_ITEM_code', 'RETURN_DATE': 'RETURNED_ITEM_DATE_date', 'RETURN_QUANTITY': 'RETURNED_ITEM_QUANTITY_quantity', 'ORDER_DETAIL_CODE': 'RETURNED_ITEM_ORDER_DETAIL_CODE_detail_code', 'RETURN_REASON_CODE': 'RETURNED_ITEM_RETURN_REASON_code', 'RETURN_DESCRIPTION_EN': 'RETURNED_ITEM_RETURN_REASON_desctiption_en'})

all_dataframes.append(returned_item)

returned_item.head()


# ### Unit

# In[10]:


unit_columns = ['UNIT_COST', 'UNIT_PRICE', 'UNIT_SALE_PRICE']
unit = go_sales_order_details
# make unit_id the primary key, autoincrement
unit['UNIT_ID'] = range(1, len(unit) + 1)
unit = unit.set_index('UNIT_ID')

# filtering the columns
unit = unit[unit_columns]

# renaming the columns
unit = unit.rename(columns={'UNIT_COST': 'UNIT_COST_cost', 'UNIT_PRICE': 'UNIT_PRICE_price', 'UNIT_SALE_PRICE': 'UNIT_SALE_sale'})

all_dataframes.append(unit)

unit.head()


# ### Sales_staff

# In[11]:


sales_staff_columns = ['SALES_STAFF_CODE', 'EMAIL', 'EXTENSION', 'POSITION_EN', 'WORK_PHONE', 'DATE_HIRED', 'MANAGER_CODE', 'FAX', 'FIRST_NAME', 'LAST_NAME',  'ADDRESS1', 'ADDRESS2', 'SALES_BRANCH_CODE']

sales_staff = pd.merge(go_staff_sales_staff, go_staff_sales_branch, left_on='SALES_BRANCH_CODE', right_on='SALES_BRANCH_CODE')

# filtering the columns
sales_staff = sales_staff[sales_staff_columns]

# renaming the columns
sales_staff = sales_staff.rename(columns={'SALES_STAFF_CODE': 'SALES_STAFF_code', 'EMAIL': 'SALES_STAFF_email', 'EXTENSION': 'SALES_STAFF_extension', 'POSITION_EN': 'SALES_STAFF_POSITION_EN_position', 'WORK_PHONE': 'SALES_STAFF_WORK_PHONE_work_phone', 'DATE_HIRED': 'SALES_STAFF_DATE_HIRED_hired', 'MANAGER_CODE': 'SALES_STAFF_MANAGER_CODE_manager', 'FAX': 'SALES_STAFF_FAX_fax', 'FIRST_NAME': 'SALES_STAFF_FIRST_NAME_first_name', 'LAST_NAME': 'SALES_STAFF_LAST_NAME_last_name', 'ADDRESS1': 'SALES_STAFF_SALES_BRANCH_ADDRESS_address', 'ADDRESS2': 'SALES_STAFF_SALES_BRANCH_ADDRESS_EXTRA_address_extra', 'SALES_BRANCH_CODE' : 'SALES_STAFF_SALES_BRANCH_CODE_branch_code'})

all_dataframes.append(sales_staff)

sales_staff.head()


# ### Satisfaction_type

# In[12]:


satisfaction_type = go_staff_satisfaction_type

# rename the columns
satisfaction_type = satisfaction_type.rename(columns={'SATISFACTION_TYPE_CODE': 'SATISFACTION_TYPE_code', 'DESCRIPTION': 'SATISFACTION_TYPE_description'})

all_dataframes.append(satisfaction_type)

satisfaction_type.head()


# ### Course

# In[13]:


course = go_staff_course

# rename the columns
course = course.rename(columns={'COURSE_CODE': 'COURSE_code', 'DESCRIPTION': 'COURSE_description'})

all_dataframes.append(course)

course.head()


# ### Year

# In[14]:


# to be used in training and satisfaction
# making year dataframe with year as the primary key
year = pd.DataFrame({'YEAR': pd.date_range(start='1/1/1990', end='1/1/2200').year})

# adding all the years from the satisfaction and training data.
year = pd.concat([year, pd.DataFrame({'YEAR': go_staff_training['YEAR']})])
year = pd.concat([year, pd.DataFrame({'YEAR': go_staff_satisfaction['YEAR']})])

# drop duplicates
year = year.drop_duplicates()

# setting the index
year = year.set_index('YEAR')

all_dataframes.append(year)

year.head()


# ### Date

# In[15]:


# to be used in order header
date = pd.DataFrame({'DATE_date': pd.date_range(start='1/1/1990', end='1/1/2200')})

# adding all the years from the order header data
date = pd.concat([date, pd.DataFrame({'DATE_date': go_sales_order_header['ORDER_DATE']})])

# drop duplicates
date = date.drop_duplicates()

# setting the index
date['DATE_date'] = pd.to_datetime(date['DATE_date']) # convert to datetime
date = date.set_index('DATE_date')

all_dataframes.append(date)

date.head()


# ### Order

# In[16]:


# to be used in order header
# making order dataframe with order_number as the primary key 
order = pd.DataFrame({'ORDER_order_number': range(1, len(go_sales_order_method) + 1)})
order['ORDER_ORDER_METHOD_CODE_method_code'] = go_sales_order_method['ORDER_METHOD_CODE']
order['ORDER_ORDER_METHOD_EN_method'] = go_sales_order_method['ORDER_METHOD_EN']

order = order.set_index('ORDER_order_number')

all_dataframes.append(order)

order.head()


# ### Retailer_site

# In[17]:


retailer_site = go_sales_retailer_site

# rename the columns
retailer_site = retailer_site.rename(columns={'RETAILER_SITE_CODE': 'RETAILER_SITE_code', 'COUNTRY_CODE': 'RETAILER_SITE_COUNTRY_CODE_country', 'CITY': 'RETAILER_SITE_CITY_city', 'REGION': 'RETAILER_SITE_REGION_region', 'POSTAL_ZONE': 'RETAILER_SITE_POSTAL_ZONE_postal_zone', 'RETAILER_CODE': 'RETAILER_SITE_RETAILER_CODE_retailer_code', 'ACTIVE_INDICATOR': 'RETAILER_SITE_ACTIVE_INDICATOR_indicator', 'ADDRESS1': 'RETAILER_SITE_ADDRESS1_address', 'ADDRESS2': 'RETAILER_SITE_ADDRESS2_address'})

all_dataframes.append(retailer_site)

retailer_site.head()


# ### Sales_branch

# In[18]:


sales_branch = go_sales_sales_branch

# rename the columns
sales_branch = sales_branch.rename(columns={'SALES_BRANCH_CODE': 'SALES_BRANCH_code', 'COUNTRY_CODE': 'SALES_BRANCH_COUNTRY_CODE_country', 'REGION': 'SALES_BRANCH_REGION_region', 'CITY': 'SALES_BRANCH_CITY_city', 'POSTAL_ZONE': 'SALES_BRANCH_POSTAL_ZONE_postal_zone', 'ADDRESS1': 'SALES_BRANCH_ADDRESS1_address', 'ADDRESS2': 'SALES_BRANCH_ADDRESS2_address', 'MAIN_ADDRESS': 'SALES_BRANCH_MAIN_ADDRESS_address'})

all_dataframes.append(sales_branch)

sales_branch.head()


# ### Retailer_contact

# In[19]:


retailer_contact = go_crm_retailer_contact

# rename the columns
retailer_contact = retailer_contact.rename(columns={'RETAILER_CONTACT_CODE': 'RETAILER_CONTACT_code', 'E_MAIL': 'RETAILER_CONTACT_email', 'RETAILER_SITE_CODE': 'RETAILER_CONTACT_RETAILER_SITE_CODE_site_code', 'JOB_POSITION_EN': 'RETAILER_CONTACT_JOB_POSITION_EN_position', 'EXTENSION': 'RETAILER_CONTACT_EXTENSION_extension', 'FAX': 'RETAILER_CONTACT_FAX_fax', 'GENDER' : 'RETAILER_CONTACT_GENDER_gender', 'FIRST_NAME': 'RETAILER_CONTACT_FIRST_NAME_first_name', 'LAST_NAME': 'RETAILER_CONTACT_LAST_NAME_last_name'})

all_dataframes.append(retailer_contact)

retailer_contact.head()


# ### Retailer

# In[20]:


retailer = pd.merge(go_crm_retailer, go_crm_retailer_type, left_on='RETAILER_TYPE_CODE', right_on='RETAILER_TYPE_CODE')

# rename the columns
retailer = retailer.rename(columns={'RETAILER_CODE': 'RETAILER_code', 'COMPANY_NAME': 'RETAILER_name', 'RETAILER_CODEMR': 'RETAILER_COMPANY_CODE_MR_company', 'RETAILER_TYPE_CODE': 'RETAILER_RETAILER_TYPE_retailer_type_code', 'RETAILER_TYPE_EN': 'RETAILER_RETAILER_TYPE_retailer_type_EN'})

all_dataframes.append(retailer)

retailer.head()


# ### Product

# In[21]:


product = pd.merge(go_sales_product, go_sales_product_type, left_on='PRODUCT_TYPE_CODE', right_on='PRODUCT_TYPE_CODE')
product = pd.merge(product, go_sales_product_line, left_on='PRODUCT_LINE_CODE', right_on='PRODUCT_LINE_CODE')

# rename the columns
product = product.rename(columns={'PRODUCT_NUMBER': 'PRODUCT_number', 'PRODUCT_NAME': 'PRODUCT_name_product', 'DESCRIPTION': 'PRODUCT_description_description', 'PRODUCT_IMAGE': 'PRODUCT_image_image', 'INTRODUCTION_DATE': 'PRODUCT_INTRODUCTION_DATE_introduced', 'PRODUCTION_COST': 'PRODUCT_PRODUCTION_COST_cost', 'MARGIN': 'PRODUCT_MARGIN_margin', 'LANGUAGE': 'PRODUCT_LANGUAGE_language',  'PRODUCT_TYPE_CODE': 'PRODUCT_PRODUCT_TYPE_code', 'PRODUCT_TYPE_EN': 'PRODUCT_PRODUCT_TYPE_code_en', 'PRODUCT_LINE_CODE': 'PRODUCT_PRODUCT_LINE_code', 'PRODUCT_LINE_EN': 'PRODUCT_PRODUCT_LINE_code_en'})

all_dataframes.append(product)

product.head()


# ### Order_details

# In[22]:


order_details = go_sales_order_details

# replacing original columns with unit and returned_item
order_details['ORDER_DETAILS_UNIT_ID_unit'] = unit.index

# merging the returned_item and order_details
order_details = pd.merge(order_details, returned_item, left_on='ORDER_DETAIL_CODE', right_on='RETURNED_ITEM_ORDER_DETAIL_CODE_detail_code')

# merging unit and order_details
order_details = pd.merge(order_details, unit, left_on='UNIT_ID', right_on='UNIT_ID')

# dropping the columns replaced by unit and returned_item
order_details = order_details.drop(columns=['UNIT_COST', 'UNIT_PRICE', 'UNIT_SALE_PRICE']) 

# rename the columns
order_details = order_details.rename(columns={'ORDER_DETAILS_CODE': 'ORDER_DETAILS_code', 'QUANTITY': 'ORDER_DETAILS_QUANTITY_quantity', 'RETURNED_ITEM_code': 'ORDER_DETAILS_RETURN_CODE_returned', 'ORDER_NUMBER': 'ORDER_DETAILS_ORDER_NUMBER_order', 'PRODUCT_NUMBER': 'ORDER_DETAILS_PRODUCT_NUMBER_product', 'UNIT_ID': 'ORDER_DETAILS_UNIT_ID_unit'})

all_dataframes.append(order_details)

order_details.head()


# ### Training

# In[23]:


training = go_staff_training

# replacing the original columns with year and course
# merging the year and course
training = pd.merge(training, year, left_on='YEAR', right_on='YEAR')
training = pd.merge(training, course, left_on='COURSE_CODE', right_on='COURSE_code')

# rename the columns
training = training.rename(columns={'SALES_STAFF_CODE': 'TRAINING_SALES_STAFF_CODE', 'COURSE_CODE': 'TRAINING_COURSE_CODE', 'YEAR': 'TRAINING_YEAR'})

all_dataframes.append(training)

training.head()


# ### Satisfaction

# In[24]:


satisfaction = go_staff_satisfaction

# replacing the original columns with year and satisfaction_type
# merging the year and satisfaction_type
satisfaction = pd.merge(satisfaction, year, left_on='YEAR', right_on='YEAR')
satisfaction = pd.merge(satisfaction, satisfaction_type, left_on='SATISFACTION_TYPE_CODE', right_on='SATISFACTION_TYPE_code')

# rename the columns
satisfaction = satisfaction.rename(columns={'SALES_STAFF_CODE': 'SATISFACTION_SALES_STAFF_CODE', 'SATISFACTION_TYPE_CODE': 'SATISFACTION_SATISFACTION_TYPE_CODE', 'YEAR': 'SATISFACTION_YEAR'})

all_dataframes.append(satisfaction)

satisfaction.head()


# ### Order_header

# In[25]:


order_header = pd.merge(go_sales_order_header, retailer, left_on='RETAILER_NAME', right_on='RETAILER_name')

# converting the date to datetime
order_header['ORDER_DATE'] = pd.to_datetime(order_header['ORDER_DATE'])

# replacing the original columns with date, sales_staff, retailer_site and retailer_contact
# merging the date, sales_staff, retailer_site and retailer_contact
order_header = pd.merge(order_header, date, left_on='ORDER_DATE', right_on='DATE_date')
order_header = pd.merge(order_header, sales_staff, left_on='SALES_STAFF_CODE', right_on='SALES_STAFF_code')
order_header = pd.merge(order_header, retailer_site, left_on='RETAILER_SITE_CODE', right_on='RETAILER_SITE_code')
order_header = pd.merge(order_header, retailer_contact, left_on='RETAILER_CONTACT_CODE', right_on='RETAILER_CONTACT_code')

# rename the columns
order_header = order_header.rename(columns={'ORDER_NUMBER': 'ORDER_HEADER_number', 'RETAILER_NAME': 'ORDER_HEADER_RETAILER_NAME', 'SALES_STAFF_CODE': 'ORDER_HEADER_SALES_STAFF', 'SALES_BRANCH_CODE': 'ORDER_HEADER_SALES_BRANCH_CODE', 'ORDER_DATE': 'ORDER_HEADER_ORDER_DATE', 'RETAILER_SITE_CODE': 'ORDER_HEADER_RETAILER_SITE_CODE', 'RETAILER_CONTACT_CODE': 'ORDER_HEADER_RETAILER_CONTACT_CODE'})

all_dataframes.append(order_header)

order_header.head()


# ### GO_SALES_INVENTORY_LEVELS

# In[26]:


go_sales_inventory_levels = go_sales_inventory_levels.rename(columns={'INVENTORY_COUNT': 'GO_SALES_INVENTORY_LEVELS_INVENTORY_COUNT', 'PRODUCT_NUMBER': 'GO_SALES_INVENTORY_LEVELS_PRODUCT_NUMBER', 'YEAR_MONTH': 'GO_SALES_INVENTORY_LEVELS_YEAR_MONTH'})

all_dataframes.append(go_sales_inventory_levels)

go_sales_inventory_levels.head()


# ### GO_SALES_PRODUCT_FORECAST

# In[27]:


go_sales_product_forecast = go_sales_product_forcast.rename(columns={'EXPECTED_VOLUME': 'GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME', 'EXPECTED_COST': 'GO_SALES_PRODUCT_FORECAST_EXPECTED_COST', 'EXPECTED_MARGIN': 'GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN', 'PRODUCT_NUMBER': 'GO_SALES_PRODUCT_FORECAST_PRODUCT_NUMBER', 'YEAR_MONTH': 'GO_SALES_PRODUCT_FORECAST_YEAR_MONTH'})

all_dataframes.append(go_sales_product_forecast)

go_sales_product_forecast.head()


# ###  SALES_TARGETData

# In[28]:


# merge product, retailer and sales_staff
go_sales_sales_target_data = pd.merge(go_sales_sales_target_data, product, left_on='PRODUCT_NUMBER', right_on='PRODUCT_number', suffixes=('_product', '_retailer'))

go_sales_sales_target_data = pd.merge(go_sales_sales_target_data, retailer, left_on='RETAILER_CODE', right_on='RETAILER_code', suffixes=('_retailer', '_sales_staff'))

# delete the original columns that are replaced by product, retailer and sales_staff
go_sales_sales_target_data = go_sales_sales_target_data.drop(columns=['PRODUCT_NUMBER', 'RETAILER_CODE', 'RETAILER_name', 'RETAILER_COMPANY_CODE_MR_company', 'RETAILER_RETAILER_TYPE_retailer_type_code', 'RETAILER_RETAILER_TYPE_retailer_type_EN'])

sales_target_data = go_sales_sales_target_data.rename(columns={'SALES_YEAR': 'SALES_TARGETDATA_SALES_YEAR', 'SALES_PERIOD': 'SALES_TARGETDATA_SALES_PERIOD', 'RETAILER_NAME': 'SALES_TARGETDATA_RETAILER_NAME', 'SALES_TARGET': 'SALES_TARGETDATA_SALES_TARGET', 'TARGET_COST': 'SALES_TARGETDATA_TARGET_COST', 'TARGET_MARGIN': 'SALES_TARGETDATA_TARGET_MARGIN', 'SALES_STAFF_CODE': 'SALES_TARGETDATA_SALES_STAFF_CODE', 'PRODUCT_NUMBER': 'SALES_TARGETDATA_PRODUCT_NUMBER', 'RETAILER_CODE': 'SALES_TARGETDATA_RETAILER_CODE'})

all_dataframes.append(sales_target_data)

sales_target_data.head()


# ### Retailer_segment

# In[29]:


retailer_segment = go_crm_retailer_segment

# rename the columns
retailer_segment = retailer_segment.rename(columns={'SEGMENT_CODE': 'RETAILER_SEGMENT_segment_code', 'LANGUAGE': 'RETAILER_SEGMENT_language', 'SEGMENT_NAME': 'RETAILER_SEGMENT_segment_name', 'SEGMENT_DESCRIPTION': 'RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'})

all_dataframes.append(retailer_segment)

retailer_segment.head()


# ### Retailer_headquarters

# In[30]:


retailer_headquarters = go_crm_retailer_headquarters

# rename the columns
retailer_headquarters = retailer_headquarters.rename(columns={'RETAILER_CODEMR': 'RETAILER_HEADQUARTER_codemr', 'RETAILER_NAME' : 'RETAIL_HEADQUARTER_retailer_name', 'ADDRESS1' : 'RETAILER_HEADQUARTER_address1_address', 'ADDRESS2' : 'RETAILER_HEADQUARTER_address2_address', 'CITY' : 'RETAILER_HEADQUARTER_city_city', 'REGION' : 'RETAILER_HEADQUARTER_region_region', 'POSTAL_ZONE' : 'RETAILER_HEADQUARTER_postal_zone_postal_zone', 'COUNTRY_CODE' : 'RETAILER_HEADQUARTER_country_code_country', 'PHONE' : 'RETAILER_HEADQUARTER_phone_phone', 'FAX': 'RETAILER_HEADQUARTER_fax_fax', 'SEGMENT_CODE' : 'RETAILER_HEADQUARTER_segment_code'})

all_dataframes.append(retailer_headquarters)

retailer_headquarters.head()


# ### Verwijderen trial kollomen

# In[31]:


for df in all_dataframes:
    trial_columns = df.filter(like='TRIAL').columns
    df.drop(columns=trial_columns, inplace=True)


# # Omzetten van de data
# Omzetten van de data naar de juiste data types.
# Year en Date zijn al omgezet naar de juiste data types.
# GO_SALES_INVENTORY_LEVELS en GO_SALES_PRODUCT_FORECAST hebben ook al de juiste datatypen

# ### Unit

# In[32]:


unit['UNIT_COST_cost'] = unit['UNIT_COST_cost'].astype('float64')
unit['UNIT_PRICE_price'] = unit['UNIT_PRICE_price'].astype('float64')
unit['UNIT_SALE_sale'] = unit['UNIT_SALE_sale'].astype('float64')

unit.dtypes


# ### Sales_staff

# In[33]:


sales_staff['SALES_STAFF_code'] = sales_staff['SALES_STAFF_code'].astype('int')
sales_staff['SALES_STAFF_email'] = sales_staff['SALES_STAFF_email'].astype('string')
sales_staff['SALES_STAFF_extension'] = sales_staff['SALES_STAFF_extension'].astype('string')
sales_staff['SALES_STAFF_POSITION_EN_position'] = sales_staff['SALES_STAFF_POSITION_EN_position'].astype('string')
sales_staff['SALES_STAFF_WORK_PHONE_work_phone'] = sales_staff['SALES_STAFF_WORK_PHONE_work_phone'].astype('string')
sales_staff['SALES_STAFF_DATE_HIRED_hired'] = pd.to_datetime(sales_staff['SALES_STAFF_DATE_HIRED_hired'], format='mixed')
sales_staff['SALES_STAFF_MANAGER_CODE_manager'] = sales_staff['SALES_STAFF_MANAGER_CODE_manager'].astype('int64')
sales_staff['SALES_STAFF_FAX_fax'] = sales_staff['SALES_STAFF_FAX_fax'].astype('string')
sales_staff['SALES_STAFF_FIRST_NAME_first_name'] = sales_staff['SALES_STAFF_FIRST_NAME_first_name'].astype('string')
sales_staff['SALES_STAFF_LAST_NAME_last_name'] = sales_staff['SALES_STAFF_LAST_NAME_last_name'].astype('string')
sales_staff['SALES_STAFF_SALES_BRANCH_CODE_branch_code'] = sales_staff['SALES_STAFF_SALES_BRANCH_CODE_branch_code'].astype('int64')
sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS_address'] = sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS_address'].astype('string')
sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS_EXTRA_address_extra'] = sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS_EXTRA_address_extra'].astype('string')

sales_staff.dtypes


# ### satisfaction_type

# In[34]:


satisfaction_type['SATISFACTION_TYPE_code'] = satisfaction_type['SATISFACTION_TYPE_code'].astype('int')
satisfaction_type['SATISFACTION_TYPE_DESCRIPTION'] = satisfaction_type['SATISFACTION_TYPE_DESCRIPTION'].astype('string')

satisfaction_type.dtypes


# ### Course

# In[35]:


course['COURSE_code'] = course['COURSE_code'].astype('int')
course['COURSE_DESCRIPTION'] = course['COURSE_DESCRIPTION'].astype('string')

course.dtypes


# ### Order

# In[36]:


order['ORDER_ORDER_METHOD_CODE_method_code'] = order['ORDER_ORDER_METHOD_CODE_method_code'].astype('int')
order['ORDER_ORDER_METHOD_EN_method'] = order['ORDER_ORDER_METHOD_EN_method'].astype('string')

order.dtypes


# ### Retailer_site

# In[37]:


retailer_site['RETAILER_SITE_code'] = retailer_site['RETAILER_SITE_code'].astype('int')
retailer_site['RETAILER_SITE_COUNTRY_CODE_country'] = retailer_site['RETAILER_SITE_COUNTRY_CODE_country'].astype('string')
retailer_site['RETAILER_SITE_CITY_city'] = retailer_site['RETAILER_SITE_CITY_city'].astype('string')
retailer_site['RETAILER_SITE_REGION_region'] = retailer_site['RETAILER_SITE_REGION_region'].astype('string')
retailer_site['RETAILER_SITE_POSTAL_ZONE_postal_zone'] = retailer_site['RETAILER_SITE_POSTAL_ZONE_postal_zone'].astype('string')
retailer_site['RETAILER_SITE_RETAILER_CODE_retailer_code'] = retailer_site['RETAILER_SITE_RETAILER_CODE_retailer_code'].astype('int')
retailer_site['RETAILER_SITE_ACTIVE_INDICATOR_indicator'] = retailer_site['RETAILER_SITE_ACTIVE_INDICATOR_indicator'].astype('int')
retailer_site['RETAILER_SITE_ADDRESS1_address'] = retailer_site['RETAILER_SITE_ADDRESS1_address'].astype('string')
retailer_site['RETAILER_SITE_ADDRESS2_address'] = retailer_site['RETAILER_SITE_ADDRESS2_address'].astype('string')

retailer_site.dtypes


# ### Sales_branch

# In[38]:


sales_branch['SALES_BRANCH_code'] = sales_branch['SALES_BRANCH_code'].astype('int')
sales_branch['SALES_BRANCH_COUNTRY_CODE_country'] = sales_branch['SALES_BRANCH_COUNTRY_CODE_country'].astype('string')
sales_branch['SALES_BRANCH_REGION_region'] = sales_branch['SALES_BRANCH_REGION_region'].astype('string')
sales_branch['SALES_BRANCH_CITY_city'] = sales_branch['SALES_BRANCH_CITY_city'].astype('string')
sales_branch['SALES_BRANCH_POSTAL_ZONE_postal_zone'] = sales_branch['SALES_BRANCH_POSTAL_ZONE_postal_zone'].astype('string')
sales_branch['SALES_BRANCH_ADDRESS1_address'] = sales_branch['SALES_BRANCH_ADDRESS1_address'].astype('string')
sales_branch['SALES_BRANCH_ADDRESS2_address'] = sales_branch['SALES_BRANCH_ADDRESS2_address'].astype('string')

sales_branch.dtypes


# ### Retailer_contact

# In[39]:


retailer_contact['RETAILER_CONTACT_code'] = retailer_contact['RETAILER_CONTACT_code'].astype('int')
retailer_contact['RETAILER_CONTACT_email'] = retailer_contact['RETAILER_CONTACT_email'].astype('string')
retailer_contact['RETAILER_CONTACT_RETAILER_SITE_CODE_site_code'] = retailer_contact['RETAILER_CONTACT_RETAILER_SITE_CODE_site_code'].astype('int')
retailer_contact['RETAILER_CONTACT_JOB_POSITION_EN_position'] = retailer_contact['RETAILER_CONTACT_JOB_POSITION_EN_position'].astype('string')
retailer_contact['RETAILER_CONTACT_EXTENSION_extension'] = retailer_contact['RETAILER_CONTACT_EXTENSION_extension'].astype('string')
retailer_contact['RETAILER_CONTACT_FAX_fax'] = retailer_contact['RETAILER_CONTACT_FAX_fax'].astype('string')
retailer_contact['RETAILER_CONTACT_GENDER_gender'] = retailer_contact['RETAILER_CONTACT_GENDER_gender'].astype('string')
retailer_contact['RETAILER_CONTACT_FIRST_NAME_first_name'] = retailer_contact['RETAILER_CONTACT_FIRST_NAME_first_name'].astype('string')
retailer_contact['RETAILER_CONTACT_LAST_NAME_last_name'] = retailer_contact['RETAILER_CONTACT_LAST_NAME_last_name'].astype('string')

retailer_contact.dtypes


# ### Retailer

# In[40]:


retailer['RETAILER_code'] = retailer['RETAILER_code'].astype('int')
retailer['RETAILER_name'] = retailer['RETAILER_name'].astype('string')
retailer['RETAILER_COMPANY_CODE_MR_company'] = retailer['RETAILER_COMPANY_CODE_MR_company'].astype('string')
retailer['RETAILER_RETAILER_TYPE_retailer_type_code'] = retailer['RETAILER_RETAILER_TYPE_retailer_type_code'].astype('int')
retailer['RETAILER_RETAILER_TYPE_retailer_type_EN'] = retailer['RETAILER_RETAILER_TYPE_retailer_type_EN'].astype('string')

retailer.dtypes


# ### Product

# In[41]:


product['PRODUCT_number'] = product['PRODUCT_number'].astype('int')
product['PRODUCT_name_product'] = product['PRODUCT_name_product'].astype('string')
product['PRODUCT_description_description'] = product['PRODUCT_description_description'].astype('string')
product['PRODUCT_image_image'] = product['PRODUCT_image_image'].astype('string')
product['PRODUCT_INTRODUCTION_DATE_introduced'] = pd.to_datetime(product['PRODUCT_INTRODUCTION_DATE_introduced'], format='mixed')
product['PRODUCT_PRODUCTION_COST_cost'] = product['PRODUCT_PRODUCTION_COST_cost'].astype('float')
product['PRODUCT_MARGIN_margin'] = product['PRODUCT_MARGIN_margin'].astype('float')
product['PRODUCT_LANGUAGE_language'] = product['PRODUCT_LANGUAGE_language'].astype('string')
product['PRODUCT_PRODUCT_LINE_code'] = product['PRODUCT_PRODUCT_LINE_code'].astype('string')
product['PRODUCT_PRODUCT_LINE_code_en'] = product['PRODUCT_PRODUCT_LINE_code_en'].astype('string')
product['PRODUCT_PRODUCT_TYPE_code'] = product['PRODUCT_PRODUCT_TYPE_code'].astype('string')
product['PRODUCT_PRODUCT_TYPE_code_en'] = product['PRODUCT_PRODUCT_TYPE_code_en'].astype('string')

product.dtypes


# ### Order_details

# In[42]:


order_details['ORDER_DETAILS_QUANTITY_quantity'] = order_details['ORDER_DETAILS_QUANTITY_quantity'].astype('int')
order_details['ORDER_DETAILS_RETURN_CODE_returned'] = order_details['ORDER_DETAILS_RETURN_CODE_returned'].astype('int')
order_details['ORDER_DETAILS_ORDER_NUMBER_order'] = order_details['ORDER_DETAILS_ORDER_NUMBER_order'].astype('int')
order_details['ORDER_DETAILS_PRODUCT_NUMBER_product'] = order_details['ORDER_DETAILS_PRODUCT_NUMBER_product'].astype('int')
order_details['RETURNED_ITEM_DATE_date'] = pd.to_datetime(order_details['RETURNED_ITEM_DATE_date'], format='mixed')
order_details['RETURNED_ITEM_QUANTITY_quantity'] = order_details['RETURNED_ITEM_QUANTITY_quantity'].astype('int')
order_details['RETURNED_ITEM_ORDER_DETAIL_CODE_detail_code'] = order_details['RETURNED_ITEM_ORDER_DETAIL_CODE_detail_code'].astype('int')
order_details['RETURNED_ITEM_RETURN_REASON_code'] = order_details['RETURNED_ITEM_RETURN_REASON_code'].astype('int')
order_details['RETURNED_ITEM_RETURN_REASON_desctiption_en'] = order_details['RETURNED_ITEM_RETURN_REASON_desctiption_en'].astype('string')
order_details['UNIT_COST_cost'] = order_details['UNIT_COST_cost'].astype('float64')
order_details['UNIT_PRICE_price'] = order_details['UNIT_PRICE_price'].astype('float64')
order_details['UNIT_SALE_sale'] = order_details['UNIT_SALE_sale'].astype('float64')

# unit is already converted
order_details.dtypes


# ### Returned_item

# In[43]:


returned_item['RETURNED_ITEM_code'] = returned_item['RETURNED_ITEM_code'].astype('int')
returned_item['RETURNED_ITEM_DATE_date'] = pd.to_datetime(returned_item['RETURNED_ITEM_DATE_date'], format='mixed')
returned_item['RETURNED_ITEM_QUANTITY_quantity'] = returned_item['RETURNED_ITEM_QUANTITY_quantity'].astype('int')
returned_item['RETURNED_ITEM_ORDER_DETAIL_CODE_detail_code'] = returned_item['RETURNED_ITEM_ORDER_DETAIL_CODE_detail_code'].astype('int')
returned_item['RETURNED_ITEM_RETURN_REASON_code'] = returned_item['RETURNED_ITEM_RETURN_REASON_code'].astype('int')
returned_item['RETURNED_ITEM_RETURN_REASON_desctiption_en'] = returned_item['RETURNED_ITEM_RETURN_REASON_desctiption_en'].astype('string')

returned_item.dtypes


# ### Sales_Targetdata

# In[44]:


sales_target_data['SALES_TARGETDATA_SALES_YEAR'] = sales_target_data['SALES_TARGETDATA_SALES_YEAR'].astype('int')
sales_target_data['SALES_TARGETDATA_SALES_PERIOD'] = sales_target_data['SALES_TARGETDATA_SALES_PERIOD'].astype('string')
sales_target_data['SALES_TARGETDATA_RETAILER_NAME'] = sales_target_data['SALES_TARGETDATA_RETAILER_NAME'].astype('string')
sales_target_data['SALES_TARGETDATA_SALES_TARGET'] = sales_target_data['SALES_TARGETDATA_SALES_TARGET'].astype('float64')
sales_target_data['SALES_TARGETDATA_SALES_STAFF_CODE'] = sales_target_data['SALES_TARGETDATA_SALES_STAFF_CODE'].astype('int')
sales_target_data['PRODUCT_number'] = sales_target_data['PRODUCT_number'].astype('int')
sales_target_data['PRODUCT_INTRODUCTION_DATE_introduced'] = pd.to_datetime(sales_target_data['PRODUCT_INTRODUCTION_DATE_introduced'], format='mixed')
sales_target_data['PRODUCT_PRODUCTION_COST_cost'] = sales_target_data['PRODUCT_PRODUCTION_COST_cost'].astype('float64')
sales_target_data['PRODUCT_PRODUCT_TYPE_code'] = sales_target_data['PRODUCT_PRODUCT_TYPE_code'].astype('string')
sales_target_data['PRODUCT_PRODUCT_TYPE_code_en'] = sales_target_data['PRODUCT_PRODUCT_TYPE_code_en'].astype('string')
sales_target_data['PRODUCT_PRODUCT_LINE_code'] = sales_target_data['PRODUCT_PRODUCT_LINE_code'].astype('string')
sales_target_data['PRODUCT_PRODUCT_LINE_code_en'] = sales_target_data['PRODUCT_PRODUCT_LINE_code_en'].astype('string')
sales_target_data['RETAILER_code'] = sales_target_data['RETAILER_code'].astype('int')
sales_target_data['PRODUCT_MARGIN_margin'] = sales_target_data['PRODUCT_MARGIN_margin'].astype('float64')
sales_target_data['PRODUCT_image_image'] = sales_target_data['PRODUCT_image_image'].astype('string')
sales_target_data['PRODUCT_LANGUAGE_language'] = sales_target_data['PRODUCT_LANGUAGE_language'].astype('string')
sales_target_data['PRODUCT_name_product'] = sales_target_data['PRODUCT_name_product'].astype('string')
sales_target_data['PRODUCT_description_description'] = sales_target_data['PRODUCT_description_description'].astype('string')


sales_target_data.dtypes


# ### Training

# In[45]:


training['TRAINING_SALES_STAFF_CODE'] = training['TRAINING_SALES_STAFF_CODE'].astype('int')
training['TRAINING_COURSE_CODE'] = training['TRAINING_COURSE_CODE'].astype('int')
training['TRAINING_YEAR'] = training['TRAINING_YEAR'].astype('int')
training['COURSE_code'] = training['COURSE_code'].astype('int')
training['COURSE_DESCRIPTION'] = training['COURSE_DESCRIPTION'].astype('string')

training.dtypes


# ### Satisfaction

# In[46]:


satisfaction['SATISFACTION_SALES_STAFF_CODE'] = satisfaction['SATISFACTION_SALES_STAFF_CODE'].astype('int')
satisfaction['SATISFACTION_SATISFACTION_TYPE_CODE'] = satisfaction['SATISFACTION_SATISFACTION_TYPE_CODE'].astype('int')
satisfaction['SATISFACTION_YEAR'] = satisfaction['SATISFACTION_YEAR'].astype('int')
satisfaction['SATISFACTION_TYPE_code'] = satisfaction['SATISFACTION_TYPE_code'].astype('int')
satisfaction['SATISFACTION_TYPE_DESCRIPTION'] = satisfaction['SATISFACTION_TYPE_DESCRIPTION'].astype('string')

satisfaction.dtypes


# 

# ### Order_header

# In[47]:


order_header['ORDER_HEADER_number'] = order_header['ORDER_HEADER_number'].astype('int')
order_header['ORDER_HEADER_RETAILER_NAME'] = order_header['ORDER_HEADER_RETAILER_NAME'].astype('string')
order_header['ORDER_HEADER_SALES_STAFF'] = order_header['ORDER_HEADER_SALES_STAFF'].astype('int')
order_header['ORDER_HEADER_SALES_BRANCH_CODE'] = order_header['ORDER_HEADER_SALES_BRANCH_CODE'].astype('int')
order_header['ORDER_HEADER_RETAILER_SITE_CODE'] = order_header['ORDER_HEADER_RETAILER_SITE_CODE'].astype('int')
order_header['ORDER_HEADER_RETAILER_CONTACT_CODE'] = order_header['ORDER_HEADER_RETAILER_CONTACT_CODE'].astype('int')
order_header['ORDER_METHOD_CODE'] = order_header['ORDER_METHOD_CODE'].astype('int')
order_header['RETAILER_CODE'] = order_header['RETAILER_code'].astype('int')
order_header['RETAILER_COMPANY_CODE_MR_company'] = order_header['RETAILER_COMPANY_CODE_MR_company'].astype('string')

order_header.dtypes


# ### Retailer_segment

# In[48]:


retailer_segment['RETAILER_SEGMENT_segment_code'] = retailer_segment['RETAILER_SEGMENT_segment_code'].astype('int')
retailer_segment['RETAILER_SEGMENT_language'] = retailer_segment['RETAILER_SEGMENT_language'].astype('string')
retailer_segment['RETAILER_SEGMENT_segment_name'] = retailer_segment['RETAILER_SEGMENT_segment_name'].astype('string')
retailer_segment['RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'] = retailer_segment['RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'].astype('string')

retailer_segment.dtypes


# ### Retailer_headquarters

# In[49]:


retailer_headquarters['RETAILER_HEADQUARTER_codemr'] = retailer_headquarters['RETAILER_HEADQUARTER_codemr'].astype('int')
retailer_headquarters['RETAIL_HEADQUARTER_retailer_name'] = retailer_headquarters['RETAIL_HEADQUARTER_retailer_name'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_address1_address'] = retailer_headquarters['RETAILER_HEADQUARTER_address1_address'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_address2_address'] = retailer_headquarters['RETAILER_HEADQUARTER_address2_address'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_city_city'] = retailer_headquarters['RETAILER_HEADQUARTER_city_city'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_region_region'] = retailer_headquarters['RETAILER_HEADQUARTER_region_region'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_postal_zone_postal_zone'] = retailer_headquarters['RETAILER_HEADQUARTER_postal_zone_postal_zone'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_country_code_country'] = retailer_headquarters['RETAILER_HEADQUARTER_country_code_country'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_phone_phone'] = retailer_headquarters['RETAILER_HEADQUARTER_phone_phone'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_fax_fax'] = retailer_headquarters['RETAILER_HEADQUARTER_fax_fax'].astype('string')
retailer_headquarters['RETAILER_HEADQUARTER_segment_code'] = retailer_headquarters['RETAILER_HEADQUARTER_segment_code'].astype('int')

retailer_headquarters.dtypes


# # Toevoegen van nieuwe kolommen
# Toevoegen van de afgeleide informatie die ook in het ETL en de data warehouse moet komen.
# Full name is al gedaan via sql

# In[50]:


sales_target_data['SALES_TARGET_DATA_TARGET_COST'] = sales_target_data['SALES_TARGETDATA_SALES_TARGET'] * sales_target_data['PRODUCT_PRODUCTION_COST_cost']


sales_target_data['SALES_TARGET_DATA_TARGET_MARGIN'] = sales_target_data['SALES_TARGETDATA_SALES_TARGET'] * sales_target_data['PRODUCT_MARGIN_margin']

order_details['ORDER_DETAILS_TOTAL_COST'] = order_details['ORDER_DETAILS_QUANTITY_quantity'] * order_details['UNIT_COST_cost']

order_details['ORDER_DETAILS_TOTAL_MARGIN'] = order_details['ORDER_DETAILS_QUANTITY_quantity'] * order_details['UNIT_SALE_sale']

go_sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_COST'] = go_sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME'] * product['PRODUCT_PRODUCTION_COST_cost']

go_sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN'] = go_sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME'] * product['PRODUCT_MARGIN_margin']

# Dimensies
product['MINIMUM_SALE_PRICE'] = product['PRODUCT_PRODUCTION_COST_cost'] + product['PRODUCT_MARGIN_margin']
returned_item['RETURNED_ITEMS_TOTAL_PRICE'] = returned_item['RETURNED_ITEM_QUANTITY_quantity'] * order_details['UNIT_SALE_sale']
course['COURSE_DESCRIPTION_SHORT'] = None
retailer_segment['SEGMENT_DESCRIPTION_SHORT'] = None
satisfaction_type['SATISFACTION_TYPE_DESCRIPTION_SHORT'] = None

# if addres2 is empty, main_adress should be addres1 and vice versa. If both adresses are available, main_adress should be null
sales_branch['MAIN_ADDRESS'] = np.where(sales_branch['SALES_BRANCH_ADDRESS2_address'].isna(), sales_branch['SALES_BRANCH_ADDRESS1_address'], np.where(sales_branch['SALES_BRANCH_ADDRESS1_address'].isna(), sales_branch['SALES_BRANCH_ADDRESS2_address'], None))

retailer_site['MAIN_ADDRESS'] = np.where(retailer_site['RETAILER_SITE_ADDRESS2_address'].isna(), retailer_site['RETAILER_SITE_ADDRESS1_address'], np.where(retailer_site['RETAILER_SITE_ADDRESS1_address'].isna(), retailer_site['RETAILER_SITE_ADDRESS2_address'], None))

retailer_headquarters['MAIN_ADDRESS'] = np.where(retailer_headquarters['RETAILER_HEADQUARTER_address2_address'].isna(), retailer_headquarters['RETAILER_HEADQUARTER_address1_address'], np.where(retailer_headquarters['RETAILER_HEADQUARTER_address1_address'].isna(), retailer_headquarters['RETAILER_HEADQUARTER_address2_address'], None))


# In[51]:


product.head()


# ## Opslaan historische data
# aan alle tabellen een kolom toevoegen met de datum van de wijziging en of dit de huidige versie is.

# In[52]:


for df in all_dataframes:
    df['LAST_UPDATED'] = pd.to_datetime('today')
    df['CURRENT'] = 1


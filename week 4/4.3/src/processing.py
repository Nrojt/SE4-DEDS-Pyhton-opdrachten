
# Practicumopdrachten Week 4.3 (eerste kans)

import pandas as pd
import pyodbc
import sqlite3
import numpy as np
import re
import sys
import os

def process():
    # database name
    DB = {
        'servername': '(local)\\SQLEXPRESS',
        'database': 'DEDS_DataWarehouse'}

    # Connect to the SQL Server database, not a specific database but the master
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=master;Trusted_Connection=yes')

    # Create a cursor object
    cur = conn.cursor()

    try:
        # Kill all active connections to the database
        cur.execute("""
            DECLARE @kill varchar(8000) = '';
            SELECT @kill = @kill + 'kill ' + convert(varchar, spid) + ';'
            FROM master.dbo.sysprocesses
            WHERE dbid = db_id('{database}')
            AND spid <> @@spid;
            EXEC(@kill);
        """.format(database=DB['database']))
        conn.commit()

        # Close the current connection
        cur.close()
        conn.close()

        # Reconnect to the SQL Server database
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=master;Trusted_Connection=yes')
        cur = conn.cursor()

        # Change the current working directory to a subfolder
        os.chdir('./week 4/4.3/src')

        # Read the SQL script file
        with open('DEDS_DataWarehouse_Creation_Script.sql', 'r') as file:
            sql_script = file.read()

        # Split the SQL script into individual statements
        sql_statements = re.split(r'GO\n', sql_script)

        # Execute the DROP DATABASE statement
        drop_database_stmt = sql_statements[0]
        cur.execute(drop_database_stmt)

        # Execute the CREATE DATABASE statement
        create_database_stmt = sql_statements[1]
        cur.execute(create_database_stmt)

        # Start a new transaction
        conn.autocommit = False

        # Execute the remaining SQL statements within the transaction
        for sql_statement in sql_statements[2:]:
            cur.execute(sql_statement)
        # Commit the transaction
        conn.commit()

    except pyodbc.Error as e:
        print(f"Error: {e}")
        conn.rollback()
        sys.exit(1)

    finally:
        # Close the cursor
        cur.close()

        # Close the connection
        conn.close()

    export_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')


    # Create a cursor from the connection
    export_cursor = export_conn.cursor()

    # check if connection is successful, else throw an error
    if export_conn:
        print("Connection with database is established")
        # setting the autocommit to True
        export_conn.autocommit = True
    else:
        print("Connection with database is not established")
        raise Exception("Connection with database is not established")

    # Connectie met sqlite databases
    go_crm_connection = sqlite3.connect('../data/raw/go_crm.sqlite')
    go_sales_connection = sqlite3.connect('../data/raw/go_sales.sqlite')
    go_staff_connection = sqlite3.connect('../data/raw/go_staff.sqlite')


    # inlezen csv bestanden
    go_sales_inventory_levels = pd.read_csv('../data/raw/GO_SALES_INVENTORY_LEVELSData.csv')
    go_sales_product_forcast = pd.read_csv('../data/raw/GO_SALES_PRODUCT_FORECASTData.csv')


    # Inlezen van de sqlite tabellen
    go_crm_retailer = pd.read_sql_query("SELECT * FROM retailer", go_crm_connection)
    go_crm_retailer_contact = pd.read_sql_query("SELECT * FROM retailer_contact", go_crm_connection)
    go_crm_retailer_headquarters = pd.read_sql_query("SELECT * FROM retailer_headquarters", go_crm_connection)
    go_crm_retailer_segment = pd.read_sql_query("SELECT * FROM retailer_segment", go_crm_connection)
    go_crm_retailer_type = pd.read_sql_query("SELECT * FROM retailer_type", go_crm_connection)
    go_crm_sales_territory = pd.read_sql_query("SELECT * FROM sales_territory", go_crm_connection)

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

    # Combineren van de brondata om tot ons ETL te komen
    all_dataframes = []

    # to be used in order details
    returned_item_columns = ['RETURN_CODE', 'RETURN_DATE', 'RETURN_QUANTITY', 'ORDER_DETAIL_CODE', 'RETURN_REASON_CODE', 'RETURN_DESCRIPTION_EN']
    returned_item = pd.merge(go_sales_returned_item, go_sales_return_reason, left_on='RETURN_REASON_CODE', right_on='RETURN_REASON_CODE')
    returned_item = pd.merge(returned_item, go_sales_order_details, left_on='ORDER_DETAIL_CODE', right_on='ORDER_DETAIL_CODE')

    # filtering the columns
    returned_item = returned_item[returned_item_columns]

    # renaming the columns
    returned_item = returned_item.rename(columns={'RETURN_CODE': 'RETURNED_ITEM_code', 'RETURN_DATE': 'RETURNED_ITEM_DATE', 'RETURN_QUANTITY': 'RETURNED_ITEM_QUANTITY', 'ORDER_DETAIL_CODE': 'RETURNED_ITEM_ORDER_DETAIL_CODE', 'RETURN_REASON_CODE': 'RETURNED_ITEM_RETURN_REASON_code', 'RETURN_DESCRIPTION_EN': 'RETURNED_ITEM_RETURN_REASON_description_en'})

    all_dataframes.append(returned_item)

    returned_item.head()


    # ### Unit

    # 
    unit_columns = ['UNIT_COST', 'UNIT_PRICE', 'UNIT_SALE_PRICE', 'UNIT_id', 'ORDER_DETAIL_CODE']
    unit = go_sales_order_details

    # make unit_id autoincrement
    unit['UNIT_id'] = range(1, len(unit) + 1)

    # filtering the columns
    unit = unit[unit_columns]

    # renaming the columns
    unit = unit.rename(columns={'UNIT_COST': 'UNIT_COST_cost', 'UNIT_PRICE': 'UNIT_PRICE_price', 'UNIT_SALE_PRICE': 'UNIT_SALE_sale'})

    all_dataframes.append(unit)

    unit.head()


    # ### Sales_staff

    # 
    sales_staff_columns = ['SALES_STAFF_CODE', 'EMAIL', 'EXTENSION', 'POSITION_EN', 'WORK_PHONE', 'DATE_HIRED', 'MANAGER_CODE', 'FAX', 'FIRST_NAME', 'LAST_NAME',  'ADDRESS1', 'ADDRESS2', 'SALES_BRANCH_CODE']

    sales_staff = pd.merge(go_staff_sales_staff, go_staff_sales_branch, left_on='SALES_BRANCH_CODE', right_on='SALES_BRANCH_CODE')

    # filtering the columns
    sales_staff = sales_staff[sales_staff_columns]

    # renaming the columns
    sales_staff = sales_staff.rename(columns={'SALES_STAFF_CODE': 'SALES_STAFF_code', 'EMAIL': 'SALES_STAFF_email', 'EXTENSION': 'SALES_STAFF_extension', 'POSITION_EN': 'SALES_STAFF_POSITION_EN_position', 'WORK_PHONE': 'SALES_STAFF_WORK_PHONE_work_phone', 'DATE_HIRED': 'SALES_STAFF_DATE_HIRED_hired', 'MANAGER_CODE': 'SALES_STAFF_MANAGER_CODE_manager', 'FAX': 'SALES_STAFF_FAX', 'FIRST_NAME': 'SALES_STAFF_FIRST_NAME_first_name', 'LAST_NAME': 'SALES_STAFF_LAST_NAME_last_name', 'ADDRESS1': 'SALES_STAFF_SALES_BRANCH_ADDRESS1_address', 'ADDRESS2': 'SALES_STAFF_SALES_BRANCH_ADDRESS2_address', 'SALES_BRANCH_CODE' : 'SALES_STAFF_SALES_BRANCH_CODE_branch_code'})

    all_dataframes.append(sales_staff)

    sales_staff.head()


    # ### Satisfaction_type

    # 
    satisfaction_type = go_staff_satisfaction_type

    # rename the columns
    satisfaction_type = satisfaction_type.rename(columns={'SATISFACTION_TYPE_CODE': 'SATISFACTION_TYPE_code', 'DESCRIPTION': 'SATISFACTION_TYPE_description'})

    all_dataframes.append(satisfaction_type)

    satisfaction_type.head()


    # ### Course

    # 
    course = go_staff_course

    # rename the columns
    course = course.rename(columns={'COURSE_CODE': 'COURSE_code', 'DESCRIPTION': 'COURSE_description'})

    all_dataframes.append(course)

    course.head()


    # ### Year

    # 
    # to be used in training and satisfaction
    # making year dataframe with year as the primary key
    year = pd.DataFrame({'YEAR': pd.date_range(start='1/1/1900', end='1/1/2200').year})

    # Drop duplicates based on 'YEAR' column
    year = year.drop_duplicates()

    # Set 'YEAR' as the index again
    year = year.set_index('YEAR')

    all_dataframes.append(year)

    year.head()


    # ### Date

    # 
    # to be used in order header
    date = pd.DataFrame({'DATE_date': pd.date_range(start='1/1/1900', end='1/1/2200')})

    # drop duplicates
    date = date.drop_duplicates()

    date = date.set_index('DATE_date')

    all_dataframes.append(date)

    date.head()


    # ### Order

    # 
    # to be used in order header
    order_columns = ['ORDER_METHOD_CODE', 'ORDER_NUMBER', 'ORDER_METHOD_EN']
    order = pd.merge(go_sales_order_header, go_sales_order_method, left_on='ORDER_METHOD_CODE', right_on='ORDER_METHOD_CODE')

    #filtering the columns
    order = order[order_columns]

    # renaming the columns
    order = order.rename(columns={'ORDER_METHOD_CODE': 'ORDER_ORDER_METHOD_CODE_method_code', 'ORDER_NUMBER': 'ORDER_order_number', 'ORDER_METHOD_EN': 'ORDER_ORDER_METHOD_EN_method'})


    all_dataframes.append(order)

    order.head()


    # ### Retailer_site

    # 
    retailer_site = go_sales_retailer_site

    # rename the columns
    retailer_site = retailer_site.rename(columns={'RETAILER_SITE_CODE': 'RETAILER_SITE_code', 'COUNTRY_CODE': 'RETAILER_SITE_COUNTRY_CODE_country', 'CITY': 'RETAILER_SITE_CITY_city', 'REGION': 'RETAILER_SITE_REGION_region', 'POSTAL_ZONE': 'RETAILER_SITE_POSTAL_ZONE_postal_zone', 'RETAILER_CODE': 'RETAILER_SITE_RETAILER_CODE_retailer_code', 'ACTIVE_INDICATOR': 'RETAILER_SITE_ACTIVE_INDICATOR_indicator', 'ADDRESS1': 'RETAILER_SITE_ADDRESS1_address', 'ADDRESS2': 'RETAILER_SITE_ADDRESS2_address'})

    all_dataframes.append(retailer_site)

    retailer_site.head()


    # ### Sales_branch

    # 
    sales_branch = go_sales_sales_branch

    # rename the columns
    sales_branch = sales_branch.rename(columns={'SALES_BRANCH_CODE': 'SALES_BRANCH_code', 'COUNTRY_CODE': 'SALES_BRANCH_COUNTRY_CODE_country', 'REGION': 'SALES_BRANCH_REGION_region', 'CITY': 'SALES_BRANCH_CITY_city', 'POSTAL_ZONE': 'SALES_BRANCH_POSTAL_ZONE_postal_zone', 'ADDRESS1': 'SALES_BRANCH_ADDRESS1_address', 'ADDRESS2': 'SALES_BRANCH_ADDRESS2_address', 'MAIN_ADDRESS': 'SALES_BRANCH_MAIN_ADDRESS_address'})

    all_dataframes.append(sales_branch)

    sales_branch.head()


    # ### Retailer_contact

    # 
    retailer_contact = go_crm_retailer_contact

    # rename the columns
    retailer_contact = retailer_contact.rename(columns={'RETAILER_CONTACT_CODE': 'RETAILER_CONTACT_code', 'E_MAIL': 'RETAILER_CONTACT_email', 'RETAILER_SITE_CODE': 'RETAILER_CONTACT_RETAILER_SITE_CODE_site_code', 'JOB_POSITION_EN': 'RETAILER_CONTACT_JOB_POSITION_EN_position', 'EXTENSION': 'RETAILER_CONTACT_EXTENSION_extension', 'FAX': 'RETAILER_CONTACT_FAX_fax', 'GENDER' : 'RETAILER_CONTACT_GENDER_gender', 'FIRST_NAME': 'RETAILER_CONTACT_FIRST_NAME_first_name', 'LAST_NAME': 'RETAILER_CONTACT_LAST_NAME_last_name'})

    all_dataframes.append(retailer_contact)

    retailer_contact.head()


    # ### Retailer


    # ### Training

    # 
    retailer = pd.merge(go_crm_retailer, go_crm_retailer_type, left_on='RETAILER_TYPE_CODE', right_on='RETAILER_TYPE_CODE')

    # rename the columns
    retailer = retailer.rename(columns={'RETAILER_CODE': 'RETAILER_code', 'COMPANY_NAME': 'RETAILER_name', 'RETAILER_CODEMR': 'RETAILER_COMPANY_CODE_MR_company', 'RETAILER_TYPE_CODE': 'RETAILER_RETAILER_TYPE_code', 'RETAILER_TYPE_EN': 'RETAILER_RETAILER_TYPE_EN'})

    all_dataframes.append(retailer)

    retailer.head()


    # ### Product

    # 
    product = pd.merge(go_sales_product, go_sales_product_type, left_on='PRODUCT_TYPE_CODE', right_on='PRODUCT_TYPE_CODE')
    product = pd.merge(product, go_sales_product_line, left_on='PRODUCT_LINE_CODE', right_on='PRODUCT_LINE_CODE')

    # rename the columns
    product = product.rename(columns={'PRODUCT_NUMBER': 'PRODUCT_number', 'PRODUCT_NAME': 'PRODUCT_name_product', 'DESCRIPTION': 'PRODUCT_description_description', 'PRODUCT_IMAGE': 'PRODUCT_image_image', 'INTRODUCTION_DATE': 'PRODUCT_INTRODUCTION_DATE_introduced', 'PRODUCTION_COST': 'PRODUCT_PRODUCTION_COST_cost', 'MARGIN': 'PRODUCT_MARGIN_margin', 'LANGUAGE': 'PRODUCT_LANGUAGE_language',  'PRODUCT_TYPE_CODE': 'PRODUCT_PRODUCT_TYPE_code', 'PRODUCT_TYPE_EN': 'PRODUCT_PRODUCT_TYPE_code_en', 'PRODUCT_LINE_CODE': 'PRODUCT_PRODUCT_LINE_code', 'PRODUCT_LINE_EN': 'PRODUCT_PRODUCT_LINE_code_en'})

    all_dataframes.append(product)

    product.head()


    # ### Order_details

    # 
    order_details = go_sales_order_details

    # merging the returned_item and order_details
    order_details = pd.merge(order_details, returned_item, left_on='ORDER_DETAIL_CODE', right_on='RETURNED_ITEM_ORDER_DETAIL_CODE')

    # merging unit and order_details
    order_details = pd.merge(order_details, unit, left_on=['ORDER_DETAIL_CODE', 'UNIT_id'], right_on=['ORDER_DETAIL_CODE', 'UNIT_id'])

    # dropping the columns replaced by unit and returned_item
    order_details = order_details.drop(columns=['UNIT_COST', 'UNIT_PRICE', 'UNIT_SALE_PRICE']) 

    # rename the columns
    order_details = order_details.rename(columns={'QUANTITY': 'ORDER_DETAILS_QUANTITY_quantity', 'RETURNED_ITEM_code': 'ORDER_DETAILS_RETURN_CODE_returned', 'ORDER_NUMBER': 'ORDER_DETAILS_ORDER_NUMBER_order', 'PRODUCT_NUMBER': 'ORDER_DETAILS_PRODUCT_NUMBER_product', 'UNIT_ID': 'ORDER_DETAILS_UNIT_ID_unit', 'ORDER_DETAIL_CODE' : 'ORDER_DETAIL_code'})

    all_dataframes.append(order_details)

    order_details.head()


    # ## training

    # 
    training = go_staff_training

    # converting the year to int 32
    training['YEAR'] = training['YEAR'].astype('int32')

    # replacing the original columns with year and course
    # merging the year and course
    training = pd.merge(training, year, left_on='YEAR', right_on='YEAR')
    training = pd.merge(training, course, left_on='COURSE_CODE', right_on='COURSE_code')

    # rename the columns
    training = training.rename(columns={'SALES_STAFF_CODE': 'TRAINING_SALES_STAFF_CODE', 'COURSE_CODE': 'TRAINING_COURSE_CODE', 'YEAR': 'TRAINING_YEAR'})

    all_dataframes.append(training)

    training.head()


    # ### Satisfaction

    # 
    satisfaction = go_staff_satisfaction

    # converting the year to int 32
    satisfaction['YEAR'] = satisfaction['YEAR'].astype('int32')

    # replacing the original columns with year and satisfaction_type
    # merging the year and satisfaction_type
    satisfaction = pd.merge(satisfaction, year, left_on='YEAR', right_on='YEAR')
    satisfaction = pd.merge(satisfaction, satisfaction_type, left_on='SATISFACTION_TYPE_CODE', right_on='SATISFACTION_TYPE_code')

    # rename the columns
    satisfaction = satisfaction.rename(columns={'SALES_STAFF_CODE': 'SATISFACTION_SALES_STAFF_CODE', 'SATISFACTION_TYPE_CODE': 'SATISFACTION_SATISFACTION_TYPE_CODE', 'YEAR': 'SATISFACTION_YEAR'})

    all_dataframes.append(satisfaction)

    satisfaction.head()


    # ### Order_header

    # 
    order_header = pd.merge(go_sales_order_header, retailer, left_on='RETAILER_NAME', right_on='RETAILER_name')

    # converting the date to datetime
    order_header['ORDER_DATE'] = pd.to_datetime(order_header['ORDER_DATE'], format='mixed')

    # replacing the original columns with date, sales_staff, retailer_site and retailer_contact
    # merging the date, sales_staff, retailer_site and retailer_contact
    order_header = pd.merge(order_header, date, left_on='ORDER_DATE', right_on='DATE_date')
    order_header = pd.merge(order_header, sales_staff, left_on='SALES_STAFF_CODE', right_on='SALES_STAFF_code')
    order_header = pd.merge(order_header, retailer_site, left_on='RETAILER_SITE_CODE', right_on='RETAILER_SITE_code')
    order_header = pd.merge(order_header, retailer_contact, left_on='RETAILER_CONTACT_CODE', right_on='RETAILER_CONTACT_code')

    # rename the columns
    order_header = order_header.rename(columns={'ORDER_NUMBER': 'ORDER_HEADER_number', 'SALES_STAFF_CODE': 'ORDER_HEADER_SALES_STAFF_CODE', 'SALES_BRANCH_CODE': 'ORDER_HEADER_SALES_BRANCH_CODE', 'ORDER_DATE': 'ORDER_HEADER_ORDER_DATE', 'RETAILER_SITE_CODE': 'ORDER_HEADER_RETAILER_SITE_CODE', 'RETAILER_CONTACT_CODE': 'ORDER_HEADER_RETAILER_CONTACT_CODE', 'RETAILER_CODE': 'ORDER_HEADER_RETAILER_CODE'})

    all_dataframes.append(order_header)

    order_header.head()


    # ### GO_SALES_INVENTORY_LEVELS

    # 
    # adding a leading 0 to the month if it is a single digit
    go_sales_inventory_levels['INVENTORY_MONTH'] = go_sales_inventory_levels['INVENTORY_MONTH'].astype(str).str.zfill(2)

    # combine year and month to one column
    go_sales_inventory_levels['YEAR_MONTH'] = go_sales_inventory_levels['INVENTORY_YEAR'].astype(str) + '-' + go_sales_inventory_levels['INVENTORY_MONTH'].astype(str)

    # dropping the original columns
    go_sales_inventory_levels = go_sales_inventory_levels.drop(columns=['INVENTORY_YEAR', 'INVENTORY_MONTH'])

    go_sales_inventory_levels = go_sales_inventory_levels.rename(columns={'INVENTORY_COUNT': 'GO_SALES_INVENTORY_LEVELS_INVENTORY_COUNT', 'PRODUCT_NUMBER': 'GO_SALES_INVENTORY_LEVELS_PRODUCT_NUMBER', 'YEAR_MONTH': 'GO_SALES_INVENTORY_LEVELS_YEAR_MONTH'})

    all_dataframes.append(go_sales_inventory_levels)

    go_sales_inventory_levels.head()


    # ### GO_SALES_PRODUCT_FORECAST

    # 
    # merging the product and go_sales_product_forecast
    product['PRODUCT_number'] = product['PRODUCT_number'].astype('int')
    sales_product_forcast = pd.merge(go_sales_product_forcast, product, left_on='PRODUCT_NUMBER', right_on='PRODUCT_number')

    # adding a leading zero to the month if it is a single digit
    sales_product_forcast['MONTH'] = sales_product_forcast['MONTH'].astype(str).str.zfill(2)

    #combine year and month to one column
    sales_product_forcast['YEAR_MONTH'] = sales_product_forcast['YEAR'].astype(str) + '-' + sales_product_forcast['MONTH'].astype(str)

    # dropping the original columns
    sales_product_forcast = sales_product_forcast.drop(columns=['YEAR', 'MONTH'])

    sales_product_forecast = sales_product_forcast.rename(columns={'EXPECTED_VOLUME': 'GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME', 'EXPECTED_COST': 'GO_SALES_PRODUCT_FORECAST_EXPECTED_COST', 'EXPECTED_MARGIN': 'GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN', 'PRODUCT_NUMBER': 'GO_SALES_PRODUCT_FORECAST_PRODUCT_NUMBER', 'YEAR_MONTH': 'GO_SALES_PRODUCT_FORECAST_YEAR_MONTH'})

    all_dataframes.append(sales_product_forecast)

    sales_product_forecast.head()


    # ##SALES_TARGETData

    # 
    # merge product, retailer and sales_staff
    product['PRODUCT_number'] = product['PRODUCT_number'].astype('int')

    go_sales_sales_target_data['PRODUCT_NUMBER'] = go_sales_sales_target_data['PRODUCT_NUMBER'].astype('int')

    sales_sales_target_data = pd.merge(go_sales_sales_target_data, product, left_on='PRODUCT_NUMBER', right_on='PRODUCT_number', suffixes=('_product', '_retailer'))

    sales_sales_target_data = pd.merge(sales_sales_target_data, retailer, left_on='RETAILER_CODE', right_on='RETAILER_code', suffixes=('_retailer', '_sales_staff'))

    # delete the original columns that are replaced by product, retailer and sales_staff
    sales_sales_target_data = sales_sales_target_data.drop(columns=['PRODUCT_NUMBER', 'RETAILER_CODE', 'RETAILER_name', 'RETAILER_COMPANY_CODE_MR_company', 'RETAILER_RETAILER_TYPE_code', 'RETAILER_RETAILER_TYPE_EN'])

    sales_target_data = sales_sales_target_data.rename(columns={'SALES_YEAR': 'SALES_TARGETDATA_SALES_YEAR', 'SALES_PERIOD': 'SALES_TARGETDATA_SALES_PERIOD', 'RETAILER_NAME': 'SALES_TARGETDATA_RETAILER_NAME', 'SALES_TARGET': 'SALES_TARGETDATA_SALES_TARGET', 'TARGET_COST': 'SALES_TARGETDATA_TARGET_COST', 'TARGET_MARGIN': 'SALES_TARGETDATA_TARGET_MARGIN', 'SALES_STAFF_CODE': 'SALES_TARGETDATA_SALES_STAFF_CODE', 'PRODUCT_NUMBER': 'SALES_TARGETDATA_PRODUCT_NUMBER', 'RETAILER_CODE': 'SALES_TARGETDATA_RETAILER_CODE'})

    all_dataframes.append(sales_target_data)

    sales_target_data.head()


    # ### Retailer_segment

    # 
    retailer_segment = go_crm_retailer_segment

    # rename the columns
    retailer_segment = retailer_segment.rename(columns={'SEGMENT_CODE': 'RETAILER_SEGMENT_segment_code', 'LANGUAGE': 'RETAILER_SEGMENT_language', 'SEGMENT_NAME': 'RETAILER_SEGMENT_segment_name', 'SEGMENT_DESCRIPTION': 'RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'})

    all_dataframes.append(retailer_segment)

    retailer_segment.head()


    # ### Retailer_headquarters

    # 
    retailer_headquarters = go_crm_retailer_headquarters

    # rename the columns
    retailer_headquarters = retailer_headquarters.rename(columns={'RETAILER_CODEMR': 'RETAILER_HEADQUARTER_codemr', 'RETAILER_NAME' : 'RETAIL_HEADQUARTER_retailer_name', 'ADDRESS1' : 'RETAILER_HEADQUARTER_address1_address', 'ADDRESS2' : 'RETAILER_HEADQUARTER_address2_address', 'CITY' : 'RETAILER_HEADQUARTER_city_city', 'REGION' : 'RETAILER_HEADQUARTER_region_region', 'POSTAL_ZONE' : 'RETAILER_HEADQUARTER_postal_zone_postal_zone', 'COUNTRY_CODE' : 'RETAILER_HEADQUARTER_country_code_country', 'PHONE' : 'RETAILER_HEADQUARTER_phone_phone', 'FAX': 'RETAILER_HEADQUARTER_fax_fax', 'SEGMENT_CODE' : 'RETAILER_HEADQUARTER_segment_code'})

    all_dataframes.append(retailer_headquarters)

    retailer_headquarters.head()


    # ### Verwijderen trial kolommen

    # 
    for df in all_dataframes:
        trial_columns = df.filter(like='TRIAL').columns
        df.drop(columns=trial_columns, inplace=True)


    # # Omzetten van de data
    # Omzetten van de data naar de juiste data types.
    # Year en Date zijn al omgezet naar de juiste data types.


    # ### Unit

    # 
    unit['UNIT_COST_cost'] = unit['UNIT_COST_cost'].astype('float64')
    unit['UNIT_PRICE_price'] = unit['UNIT_PRICE_price'].astype('float64')
    unit['UNIT_SALE_sale'] = unit['UNIT_SALE_sale'].astype('float64')

    unit.dtypes


    # ### Sales_staff

    # 
    sales_staff['SALES_STAFF_code'] = sales_staff['SALES_STAFF_code'].astype('int')
    sales_staff['SALES_STAFF_email'] = sales_staff['SALES_STAFF_email'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_extension'] = sales_staff['SALES_STAFF_extension'].astype('string').replace({np.nan: None})

    sales_staff['SALES_STAFF_POSITION_EN_position'] = sales_staff['SALES_STAFF_POSITION_EN_position'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_WORK_PHONE_work_phone'] = sales_staff['SALES_STAFF_WORK_PHONE_work_phone'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_DATE_HIRED_hired'] = pd.to_datetime(sales_staff['SALES_STAFF_DATE_HIRED_hired'], format='mixed')
    sales_staff['SALES_STAFF_MANAGER_CODE_manager'] = sales_staff['SALES_STAFF_MANAGER_CODE_manager'].astype('int64')
    sales_staff['SALES_STAFF_FAX'] = sales_staff['SALES_STAFF_FAX'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_FIRST_NAME_first_name'] = sales_staff['SALES_STAFF_FIRST_NAME_first_name'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_LAST_NAME_last_name'] = sales_staff['SALES_STAFF_LAST_NAME_last_name'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_SALES_BRANCH_CODE_branch_code'] = sales_staff['SALES_STAFF_SALES_BRANCH_CODE_branch_code'].astype('int64')
    sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS1_address'] = sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS1_address'].astype('string').replace({np.nan: None})
    sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS2_address'] = sales_staff['SALES_STAFF_SALES_BRANCH_ADDRESS2_address'].astype('string').replace({np.nan: None})


    sales_staff.head()


    # ### satisfaction_type

    # 
    satisfaction_type['SATISFACTION_TYPE_code'] = satisfaction_type['SATISFACTION_TYPE_code'].astype('int')
    satisfaction_type['SATISFACTION_TYPE_DESCRIPTION'] = satisfaction_type['SATISFACTION_TYPE_DESCRIPTION'].astype('string').replace({np.nan: None})

    satisfaction_type.dtypes


    # ### Course

    # 
    course['COURSE_code'] = course['COURSE_code'].astype('int')
    course['COURSE_DESCRIPTION'] = course['COURSE_DESCRIPTION'].astype('string').replace({np.nan: None})

    course.dtypes


    # ### Order

    # 
    order['ORDER_ORDER_METHOD_CODE_method_code'] = order['ORDER_ORDER_METHOD_CODE_method_code'].astype('int')
    order['ORDER_order_number'] = order['ORDER_order_number'].astype('int')
    order['ORDER_ORDER_METHOD_EN_method'] = order['ORDER_ORDER_METHOD_EN_method'].astype('string').replace({np.nan: None})

    order.dtypes


    # ### Retailer_site

    # 
    retailer_site['RETAILER_SITE_code'] = retailer_site['RETAILER_SITE_code'].astype('int')
    retailer_site['RETAILER_SITE_COUNTRY_CODE_country'] = retailer_site['RETAILER_SITE_COUNTRY_CODE_country'].astype('string').replace({np.nan: None})
    retailer_site['RETAILER_SITE_CITY_city'] = retailer_site['RETAILER_SITE_CITY_city'].astype('string').replace({np.nan: None})
    retailer_site['RETAILER_SITE_REGION_region'] = retailer_site['RETAILER_SITE_REGION_region'].astype('string').replace({np.nan: None})
    retailer_site['RETAILER_SITE_POSTAL_ZONE_postal_zone'] = retailer_site['RETAILER_SITE_POSTAL_ZONE_postal_zone'].astype('string').replace({np.nan: None})
    retailer_site['RETAILER_SITE_RETAILER_CODE_retailer_code'] = retailer_site['RETAILER_SITE_RETAILER_CODE_retailer_code'].astype('int')
    retailer_site['RETAILER_SITE_ACTIVE_INDICATOR_indicator'] = retailer_site['RETAILER_SITE_ACTIVE_INDICATOR_indicator'].astype('int')
    retailer_site['RETAILER_SITE_ADDRESS1_address'] = retailer_site['RETAILER_SITE_ADDRESS1_address'].astype('string').replace({np.nan: None})
    retailer_site['RETAILER_SITE_ADDRESS2_address'] = retailer_site['RETAILER_SITE_ADDRESS2_address'].astype('string').replace({np.nan: None})


    retailer_site.dtypes


    # ### Sales_branch

    # 
    sales_branch['SALES_BRANCH_code'] = sales_branch['SALES_BRANCH_code'].astype('int')
    sales_branch['SALES_BRANCH_COUNTRY_CODE_country'] = sales_branch['SALES_BRANCH_COUNTRY_CODE_country'].astype('string').replace({np.nan: None})
    sales_branch['SALES_BRANCH_REGION_region'] = sales_branch['SALES_BRANCH_REGION_region'].astype('string').replace({np.nan: None})
    sales_branch['SALES_BRANCH_CITY_city'] = sales_branch['SALES_BRANCH_CITY_city'].astype('string').replace({np.nan: None})
    sales_branch['SALES_BRANCH_POSTAL_ZONE_postal_zone'] = sales_branch['SALES_BRANCH_POSTAL_ZONE_postal_zone'].astype('string').replace({np.nan: None})
    sales_branch['SALES_BRANCH_ADDRESS1_address'] = sales_branch['SALES_BRANCH_ADDRESS1_address'].astype('string').replace({np.nan: None})
    sales_branch['SALES_BRANCH_ADDRESS2_address'] = sales_branch['SALES_BRANCH_ADDRESS2_address'].astype('string').replace({np.nan: None})

    sales_branch.dtypes


    # ### Retailer_contact

    # 
    retailer_contact['RETAILER_CONTACT_code'] = retailer_contact['RETAILER_CONTACT_code'].astype('int')
    retailer_contact['RETAILER_CONTACT_email'] = retailer_contact['RETAILER_CONTACT_email'].astype('string').replace({np.nan: None})
    retailer_contact['RETAILER_CONTACT_RETAILER_SITE_CODE_site_code'] = retailer_contact['RETAILER_CONTACT_RETAILER_SITE_CODE_site_code'].astype('int')
    retailer_contact['RETAILER_CONTACT_JOB_POSITION_EN_position'] = retailer_contact['RETAILER_CONTACT_JOB_POSITION_EN_position'].astype('string').replace({np.nan: None})
    retailer_contact['RETAILER_CONTACT_EXTENSION_extension'] = retailer_contact['RETAILER_CONTACT_EXTENSION_extension'].astype('string').replace({np.nan: None})
    retailer_contact['RETAILER_CONTACT_FAX_fax'] = retailer_contact['RETAILER_CONTACT_FAX_fax'].astype('string').replace({np.nan: None})
    retailer_contact['RETAILER_CONTACT_GENDER_gender'] = retailer_contact['RETAILER_CONTACT_GENDER_gender'].astype('string').replace({np.nan: None})
    retailer_contact['RETAILER_CONTACT_FIRST_NAME_first_name'] = retailer_contact['RETAILER_CONTACT_FIRST_NAME_first_name'].astype('string').replace({np.nan: None})
    retailer_contact['RETAILER_CONTACT_LAST_NAME_last_name'] = retailer_contact['RETAILER_CONTACT_LAST_NAME_last_name'].astype('string').replace({np.nan: None})

    retailer_contact.dtypes


    # ### Retailer

    # 
    retailer['RETAILER_code'] = retailer['RETAILER_code'].astype('int')
    retailer['RETAILER_name'] = retailer['RETAILER_name'].astype('string').replace({np.nan: None})
    retailer['RETAILER_COMPANY_CODE_MR_company'] = retailer['RETAILER_COMPANY_CODE_MR_company'].astype('string').replace({np.nan: None})
    retailer['RETAILER_RETAILER_TYPE_code'] = retailer['RETAILER_RETAILER_TYPE_code'].astype('int')
    retailer['RETAILER_RETAILER_TYPE_EN'] = retailer['RETAILER_RETAILER_TYPE_EN'].astype('string').replace({np.nan: None})

    retailer.dtypes


    # ### Product

    # 
    product['PRODUCT_number'] = product['PRODUCT_number'].astype('int')
    product['PRODUCT_name_product'] = product['PRODUCT_name_product'].astype('string').replace({np.nan: None})
    product['PRODUCT_description_description'] = product['PRODUCT_description_description'].astype('string').replace({np.nan: None})
    product['PRODUCT_image_image'] = product['PRODUCT_image_image'].astype('string').replace({np.nan: None})
    product['PRODUCT_INTRODUCTION_DATE_introduced'] = pd.to_datetime(product['PRODUCT_INTRODUCTION_DATE_introduced'], format='mixed')
    product['PRODUCT_PRODUCTION_COST_cost'] = product['PRODUCT_PRODUCTION_COST_cost'].astype('float')
    product['PRODUCT_MARGIN_margin'] = product['PRODUCT_MARGIN_margin'].astype('float')
    product['PRODUCT_LANGUAGE_language'] = product['PRODUCT_LANGUAGE_language'].astype('string').replace({np.nan: None})
    product['PRODUCT_PRODUCT_LINE_code'] = product['PRODUCT_PRODUCT_LINE_code'].astype('string').replace({np.nan: None})
    product['PRODUCT_PRODUCT_LINE_code_en'] = product['PRODUCT_PRODUCT_LINE_code_en'].astype('string').replace({np.nan: None})
    product['PRODUCT_PRODUCT_TYPE_code'] = product['PRODUCT_PRODUCT_TYPE_code'].astype('string').replace({np.nan: None})
    product['PRODUCT_PRODUCT_TYPE_code_en'] = product['PRODUCT_PRODUCT_TYPE_code_en'].astype('string').replace({np.nan: None})

    product.dtypes


    # ### Order_details

    # 
    order_details['ORDER_DETAILS_QUANTITY_quantity'] = order_details['ORDER_DETAILS_QUANTITY_quantity'].astype('int')
    order_details['ORDER_DETAILS_RETURN_CODE_returned'] = order_details['ORDER_DETAILS_RETURN_CODE_returned'].astype('int')
    order_details['ORDER_DETAILS_ORDER_NUMBER_order'] = order_details['ORDER_DETAILS_ORDER_NUMBER_order'].astype('int')
    order_details['ORDER_DETAILS_PRODUCT_NUMBER_product'] = order_details['ORDER_DETAILS_PRODUCT_NUMBER_product'].astype('int')
    order_details['RETURNED_ITEM_DATE'] = pd.to_datetime(order_details['RETURNED_ITEM_DATE'], format='mixed')
    order_details['RETURNED_ITEM_QUANTITY'] = order_details['RETURNED_ITEM_QUANTITY'].astype('int')
    order_details['RETURNED_ITEM_ORDER_DETAIL_CODE'] = order_details['RETURNED_ITEM_ORDER_DETAIL_CODE'].astype('int')
    order_details['RETURNED_ITEM_RETURN_REASON_code'] = order_details['RETURNED_ITEM_RETURN_REASON_code'].astype('int')
    order_details['RETURNED_ITEM_RETURN_REASON_description_en'] = order_details['RETURNED_ITEM_RETURN_REASON_description_en'].astype('string').replace({np.nan: None})
    order_details['UNIT_COST_cost'] = order_details['UNIT_COST_cost'].astype('float64')
    order_details['UNIT_PRICE_price'] = order_details['UNIT_PRICE_price'].astype('float64')
    order_details['UNIT_SALE_sale'] = order_details['UNIT_SALE_sale'].astype('float64')
    order_details['ORDER_DETAIL_code'] = order_details['ORDER_DETAIL_code'].astype('int')

    # unit is already converted
    order_details.dtypes


    # ### Returned_item

    # 
    returned_item['RETURNED_ITEM_code'] = returned_item['RETURNED_ITEM_code'].astype('int')
    returned_item['RETURNED_ITEM_DATE'] = pd.to_datetime(returned_item['RETURNED_ITEM_DATE'], format='mixed')
    returned_item['RETURNED_ITEM_QUANTITY'] = returned_item['RETURNED_ITEM_QUANTITY'].astype('int')
    returned_item['RETURNED_ITEM_ORDER_DETAIL_CODE'] = returned_item['RETURNED_ITEM_ORDER_DETAIL_CODE'].astype('int')
    returned_item['RETURNED_ITEM_RETURN_REASON_code'] = returned_item['RETURNED_ITEM_RETURN_REASON_code'].astype('int')
    returned_item['RETURNED_ITEM_RETURN_REASON_description_en'] = returned_item['RETURNED_ITEM_RETURN_REASON_description_en'].astype('string').replace({np.nan: None})

    returned_item.dtypes


    # ### Sales_Targetdata

    # 
    sales_target_data['SALES_TARGETDATA_SALES_YEAR'] = sales_target_data['SALES_TARGETDATA_SALES_YEAR'].astype('int')
    sales_target_data['SALES_TARGETDATA_SALES_PERIOD'] = sales_target_data['SALES_TARGETDATA_SALES_PERIOD'].astype('string').replace({np.nan: None})
    sales_target_data['SALES_TARGETDATA_RETAILER_NAME'] = sales_target_data['SALES_TARGETDATA_RETAILER_NAME'].astype('string').replace({np.nan: None})
    sales_target_data['SALES_TARGETDATA_SALES_TARGET'] = sales_target_data['SALES_TARGETDATA_SALES_TARGET'].astype('float64')
    sales_target_data['SALES_TARGETDATA_SALES_STAFF_CODE'] = sales_target_data['SALES_TARGETDATA_SALES_STAFF_CODE'].astype('int')
    sales_target_data['PRODUCT_number'] = sales_target_data['PRODUCT_number'].astype('int')
    sales_target_data['PRODUCT_INTRODUCTION_DATE_introduced'] = pd.to_datetime(sales_target_data['PRODUCT_INTRODUCTION_DATE_introduced'], format='mixed')
    sales_target_data['PRODUCT_PRODUCTION_COST_cost'] = sales_target_data['PRODUCT_PRODUCTION_COST_cost'].astype('float64')
    sales_target_data['PRODUCT_PRODUCT_TYPE_code'] = sales_target_data['PRODUCT_PRODUCT_TYPE_code'].astype('string').replace({np.nan: None})
    sales_target_data['PRODUCT_PRODUCT_TYPE_code_en'] = sales_target_data['PRODUCT_PRODUCT_TYPE_code_en'].astype('string').replace({np.nan: None})
    sales_target_data['PRODUCT_PRODUCT_LINE_code'] = sales_target_data['PRODUCT_PRODUCT_LINE_code'].astype('string').replace({np.nan: None})
    sales_target_data['PRODUCT_PRODUCT_LINE_code_en'] = sales_target_data['PRODUCT_PRODUCT_LINE_code_en'].astype('string').replace({np.nan: None})
    sales_target_data['RETAILER_code'] = sales_target_data['RETAILER_code'].astype('int')
    sales_target_data['PRODUCT_MARGIN_margin'] = sales_target_data['PRODUCT_MARGIN_margin'].astype('float64')
    sales_target_data['PRODUCT_image_image'] = sales_target_data['PRODUCT_image_image'].astype('string').replace({np.nan: None})
    sales_target_data['PRODUCT_LANGUAGE_language'] = sales_target_data['PRODUCT_LANGUAGE_language'].astype('string').replace({np.nan: None})
    sales_target_data['PRODUCT_name_product'] = sales_target_data['PRODUCT_name_product'].astype('string').replace({np.nan: None})
    sales_target_data['PRODUCT_description_description'] = sales_target_data['PRODUCT_description_description'].astype('string').replace({np.nan: None})

    sales_target_data.dtypes


    # ### Training

    # 
    training['TRAINING_SALES_STAFF_CODE'] = training['TRAINING_SALES_STAFF_CODE'].astype('int')
    training['TRAINING_COURSE_CODE'] = training['TRAINING_COURSE_CODE'].astype('int')
    training['TRAINING_YEAR'] = training['TRAINING_YEAR'].astype('int')
    training['COURSE_code'] = training['COURSE_code'].astype('int')
    training['COURSE_DESCRIPTION'] = training['COURSE_DESCRIPTION'].astype('string').replace({np.nan: None})

    training.dtypes


    # ### Satisfaction

    # 
    satisfaction['SATISFACTION_SALES_STAFF_CODE'] = satisfaction['SATISFACTION_SALES_STAFF_CODE'].astype('int')
    satisfaction['SATISFACTION_SATISFACTION_TYPE_CODE'] = satisfaction['SATISFACTION_SATISFACTION_TYPE_CODE'].astype('int')
    satisfaction['SATISFACTION_YEAR'] = satisfaction['SATISFACTION_YEAR'].astype('int')
    satisfaction['SATISFACTION_TYPE_code'] = satisfaction['SATISFACTION_TYPE_code'].astype('int')
    satisfaction['SATISFACTION_TYPE_DESCRIPTION'] = satisfaction['SATISFACTION_TYPE_DESCRIPTION'].astype('string').replace({np.nan: None})

    satisfaction.dtypes


    # 


    # ### Order_header

    # 
    order_header['ORDER_HEADER_number'] = order_header['ORDER_HEADER_number'].astype('int')
    order_header['ORDER_HEADER_SALES_STAFF_CODE'] = order_header['ORDER_HEADER_SALES_STAFF_CODE'].astype('int')
    order_header['ORDER_HEADER_SALES_BRANCH_CODE'] = order_header['ORDER_HEADER_SALES_BRANCH_CODE'].astype('int')
    order_header['ORDER_HEADER_RETAILER_SITE_CODE'] = order_header['ORDER_HEADER_RETAILER_SITE_CODE'].astype('int')
    order_header['ORDER_HEADER_RETAILER_CONTACT_CODE'] = order_header['ORDER_HEADER_RETAILER_CONTACT_CODE'].astype('int')
    order_header['ORDER_METHOD_CODE'] = order_header['ORDER_METHOD_CODE'].astype('int')
    order_header['RETAILER_CODE'] = order_header['RETAILER_code'].astype('int')
    order_header['RETAILER_COMPANY_CODE_MR_company'] = order_header['RETAILER_COMPANY_CODE_MR_company'].astype('string').replace({np.nan: None})
    order_header['RETAILER_NAME'] = order_header['RETAILER_name'].astype('string').replace({np.nan: None})

    order_header.dtypes


    # ### Retailer_segment

    # 
    retailer_segment['RETAILER_SEGMENT_segment_code'] = retailer_segment['RETAILER_SEGMENT_segment_code'].astype('int')
    retailer_segment['RETAILER_SEGMENT_language'] = retailer_segment['RETAILER_SEGMENT_language'].astype('string').replace({np.nan: None})
    retailer_segment['RETAILER_SEGMENT_segment_name'] = retailer_segment['RETAILER_SEGMENT_segment_name'].astype('string').replace({np.nan: None})
    retailer_segment['RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'] = retailer_segment['RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'].astype('string').replace({np.nan: None})

    retailer_segment.dtypes


    # ### Retailer_headquarters

    # 
    retailer_headquarters['RETAILER_HEADQUARTER_codemr'] = retailer_headquarters['RETAILER_HEADQUARTER_codemr'].astype('int')
    retailer_headquarters['RETAIL_HEADQUARTER_retailer_name'] = retailer_headquarters['RETAIL_HEADQUARTER_retailer_name'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_address1_address'] = retailer_headquarters['RETAILER_HEADQUARTER_address1_address'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_address2_address'] = retailer_headquarters['RETAILER_HEADQUARTER_address2_address'].astype('string').replace({np.nan: None})

    retailer_headquarters['RETAILER_HEADQUARTER_city_city'] = retailer_headquarters['RETAILER_HEADQUARTER_city_city'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_region_region'] = retailer_headquarters['RETAILER_HEADQUARTER_region_region'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_postal_zone_postal_zone'] = retailer_headquarters['RETAILER_HEADQUARTER_postal_zone_postal_zone'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_country_code_country'] = retailer_headquarters['RETAILER_HEADQUARTER_country_code_country'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_phone_phone'] = retailer_headquarters['RETAILER_HEADQUARTER_phone_phone'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_fax_fax'] = retailer_headquarters['RETAILER_HEADQUARTER_fax_fax'].astype('string').replace({np.nan: None})
    retailer_headquarters['RETAILER_HEADQUARTER_segment_code'] = retailer_headquarters['RETAILER_HEADQUARTER_segment_code'].astype('int')

    retailer_headquarters

    #retailer_headquarters.dtypes


    # # Toevoegen van nieuwe kolommen
    # Toevoegen van de afgeleide informatie die ook in het ETL en de data warehouse moet komen.
    # Full name is al gedaan via sql

    # 
    sales_target_data['SALES_TARGET_DATA_TARGET_COST'] = sales_target_data['SALES_TARGETDATA_SALES_TARGET'] * sales_target_data['PRODUCT_PRODUCTION_COST_cost']

    sales_target_data['SALES_TARGET_DATA_TARGET_MARGIN'] = sales_target_data['SALES_TARGETDATA_SALES_TARGET'] * sales_target_data['PRODUCT_MARGIN_margin']

    order_details['ORDER_DETAILS_TOTAL_COST_total'] = order_details['ORDER_DETAILS_QUANTITY_quantity'] * order_details['UNIT_COST_cost']

    order_details['ORDER_DETAILS_TOTAL_MARGIN_margin'] = order_details['ORDER_DETAILS_QUANTITY_quantity'] * order_details['UNIT_SALE_sale']

    sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_COST'] = sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME'].astype('int') * sales_product_forecast['PRODUCT_PRODUCTION_COST_cost'].astype('float')
    sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN'] = sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME'].astype('int') * sales_product_forcast['PRODUCT_MARGIN_margin'].astype('float')

    # Dimensies
    returned_item['RETURNED_ITEM_RETURNED_ITEMS_TOTAL_PRICE'] = returned_item['RETURNED_ITEM_QUANTITY'] * order_details['UNIT_SALE_sale']
    course['COURSE_DESCRIPTION_SHORT'] = None
    retailer_segment['SEGMENT_DESCRIPTION_SHORT'] = None
    satisfaction_type['SATISFACTION_TYPE_DESCRIPTION_SHORT'] = None

    # if address2 is empty, main_address should be address1 and vice versa. If both addresses are available, main_address should be null
    sales_branch['MAIN_ADDRESS'] = np.where(sales_branch['SALES_BRANCH_ADDRESS2_address'].isna(), sales_branch['SALES_BRANCH_ADDRESS1_address'], np.where(sales_branch['SALES_BRANCH_ADDRESS1_address'].isna(), sales_branch['SALES_BRANCH_ADDRESS2_address'], None))

    retailer_site['MAIN_ADDRESS'] = np.where(retailer_site['RETAILER_SITE_ADDRESS2_address'].isna(), retailer_site['RETAILER_SITE_ADDRESS1_address'], np.where(retailer_site['RETAILER_SITE_ADDRESS1_address'].isna(), retailer_site['RETAILER_SITE_ADDRESS2_address'], None))

    retailer_headquarters['MAIN_ADDRESS'] = np.where(retailer_headquarters['RETAILER_HEADQUARTER_address2_address'].isna(), retailer_headquarters['RETAILER_HEADQUARTER_address1_address'], np.where(retailer_headquarters['RETAILER_HEADQUARTER_address1_address'].isna(), retailer_headquarters['RETAILER_HEADQUARTER_address2_address'], None))



    # ### GO_SALES_PRODUCT_FORECAST

    # 
    sales_product_forcast['YEAR_MONTH'] = sales_product_forcast['YEAR_MONTH'].astype('string')

    sales_product_forcast.dtypes


    # ## Opslaan historische data
    # Aan alle tabellen een kolom toevoegen met de datum van de wijziging en of dit de huidige versie is.

    # 
    for df in all_dataframes:
        df['LAST_UPDATED'] = pd.to_datetime('today')
        df['CURRENT'] = 1


    # 


    # # Data opslaan in de database
    # De pandas dataframes opslaan in de mssql database


    # ## year

    # 
    print(f"Rows: {year.shape[0]}")
    for index, row in year.iterrows():
        query = 'INSERT INTO Year (YEAR) VALUES (?)'
        values = index
        export_cursor.execute(query, values)


    # ## Unit

    # 
    print(f"Rows: {unit.shape[0]}")

    unit_code_sk = {}

    for index, row in unit.iterrows():
        query = 'INSERT INTO Unit (UNIT_id, UNIT_COST_cost, UNIT_PRICE_price, UNIT_SALE_sale) VALUES (?,?,?,?);'
        values = (row['UNIT_id'], row['UNIT_COST_cost'], row['UNIT_PRICE_price'], row['UNIT_SALE_sale'])
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        unit_code_sk[row['UNIT_id']] = generated_primary_key



    # ## date

    # 
    print(f"Rows: {date.shape[0]}")
    for index, row in date.iterrows():
        query = 'INSERT INTO Date (DATE_date) VALUES (?)'
        values = index
        export_cursor.execute(query, values)


    # ## Sales_staff

    # 
    print(f"Rows: {sales_staff.shape[0]}")

    sales_staff_code_sk = {}

    for index, row in sales_staff.iterrows():
        query = 'INSERT INTO Sales_staff (SALES_STAFF_code, SALES_STAFF_email, SALES_STAFF_extension, SALES_STAFF_POSITION_EN_position, SALES_STAFF_WORK_PHONE_work_phone, SALES_STAFF_DATE_HIRED_hired, SALES_STAFF_FAX, SALES_STAFF_FIRST_NAME_first_name, SALES_STAFF_LAST_NAME_last_name, SALES_STAFF_SALES_BRANCH_CODE_branch_code, SALES_STAFF_SALES_BRANCH_ADDRESS1_address, SALES_STAFF_SALES_BRANCH_ADDRESS2_address) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'

        values = (row['SALES_STAFF_code'], row['SALES_STAFF_email'], row['SALES_STAFF_extension'], row['SALES_STAFF_POSITION_EN_position'], row['SALES_STAFF_WORK_PHONE_work_phone'], row['SALES_STAFF_DATE_HIRED_hired'], row['SALES_STAFF_FAX'], row['SALES_STAFF_FIRST_NAME_first_name'], row['SALES_STAFF_LAST_NAME_last_name'], row['SALES_STAFF_SALES_BRANCH_CODE_branch_code'], row['SALES_STAFF_SALES_BRANCH_ADDRESS1_address'], row['SALES_STAFF_SALES_BRANCH_ADDRESS2_address'])

        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        sales_staff_code_sk[row['SALES_STAFF_code']] = generated_primary_key

    # foreign key manager SALES_STAFF_MANAGER_CODE_manager
    for index, row in sales_staff.iterrows():
            if pd.notnull(row['SALES_STAFF_MANAGER_CODE_manager']):
                query = 'UPDATE Sales_staff SET SALES_STAFF_MANAGER_CODE_manager = ? WHERE SALES_STAFF_SK = ?'
                values = (sales_staff_code_sk[row['SALES_STAFF_MANAGER_CODE_manager']], sales_staff_code_sk[row['SALES_STAFF_code']])
                export_cursor.execute(query, values)


    # ## satisfaction_type

    # 
    print(f"Rows: {satisfaction_type.shape[0]}")

    satisfaction_type_code_sk = {}

    for index, row in satisfaction_type.iterrows():
        query = 'INSERT INTO Satisfaction_type (SATISFACTION_TYPE_code, SATISFACTION_TYPE_DESCRIPTION, CURRENT_VALUE) VALUES (?,?,?)'
        values = (row['SATISFACTION_TYPE_code'], row['SATISFACTION_TYPE_DESCRIPTION'], 1)
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        satisfaction_type_code_sk[row['SATISFACTION_TYPE_code']] = generated_primary_key


    # ## Course

    # 
    print(f"Rows: {course.shape[0]}")

    course_code_sk = {}

    for index, row in course.iterrows():
        query = 'INSERT INTO Course (COURSE_code, COURSE_DESCRIPTION, CURRENT_VALUE) VALUES (?,?,?)'
        values = (row['COURSE_code'], row['COURSE_DESCRIPTION'], 1)
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        course_code_sk[row['COURSE_code']] = generated_primary_key


    # ## order

    # 
    print(f"Rows: {order.shape[0]}")

    order_code_sk = {}

    for index, row in order.iterrows():
        query = 'INSERT INTO "Order" (ORDER_order_number, ORDER_ORDER_METHOD_CODE_method_code, ORDER_ORDER_METHOD_EN_method) VALUES (?,?,?)'
        values = (row['ORDER_order_number'], row['ORDER_ORDER_METHOD_CODE_method_code'], row['ORDER_ORDER_METHOD_EN_method'])
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        order_code_sk[row['ORDER_order_number']] = generated_primary_key


    # ## retailer_site

    # 
    print(f"Rows: {retailer_site.shape[0]}")

    # dictionary to store the primary key with the surrogate key. To be used for fk reference
    retailer_site_code_sk_dict = {}

    for index, row in retailer_site.iterrows():
        query = 'INSERT INTO Retailer_site (RETAILER_SITE_code, RETAILER_SITE_COUNTRY_CODE_country, RETAILER_SITE_CITY_city, RETAILER_SITE_REGION_region, RETAILER_SITE_POSTAL_ZONE_postal_zone, RETAILER_SITE_RETAILER_CODE_retailer_code, RETAILER_SITE_ACTIVE_INDICATOR_indicator, RETAILER_SITE_ADDRESS1_address, RETAILER_SITE_ADDRESS2_address) VALUES (?,?,?,?,?,?,?,?,?)'
        values = (row['RETAILER_SITE_code'], row['RETAILER_SITE_COUNTRY_CODE_country'], row['RETAILER_SITE_CITY_city'], row['RETAILER_SITE_REGION_region'], row['RETAILER_SITE_POSTAL_ZONE_postal_zone'], row['RETAILER_SITE_RETAILER_CODE_retailer_code'], row['RETAILER_SITE_ACTIVE_INDICATOR_indicator'], row['RETAILER_SITE_ADDRESS1_address'], row['RETAILER_SITE_ADDRESS2_address'])
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        retailer_site_code_sk_dict[row['RETAILER_SITE_code']] = generated_primary_key


    # ## sales_branch

    # 
    print(f"Rows: {sales_branch.shape[0]}")

    sales_branch_code_sk = {}

    for index, row in sales_branch.iterrows():
        query = 'INSERT INTO Sales_branch (SALES_BRANCH_code, SALES_BRANCH_COUNTRY_CODE_country, SALES_BRANCH_REGION_region, SALES_BRANCH_CITY_city, SALES_BRANCH_POSTAL_ZONE_postal_zone, SALES_BRANCH_ADDRESS1_address, SALES_BRANCH_ADDRESS2_address) VALUES (?,?,?,?,?,?,?)'
        values = (row['SALES_BRANCH_code'], row['SALES_BRANCH_COUNTRY_CODE_country'], row['SALES_BRANCH_REGION_region'], row['SALES_BRANCH_CITY_city'], row['SALES_BRANCH_POSTAL_ZONE_postal_zone'], row['SALES_BRANCH_ADDRESS1_address'], row['SALES_BRANCH_ADDRESS2_address'])
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        sales_branch_code_sk[row['SALES_BRANCH_code']] = generated_primary_key


    # ## retailer_contact

    # 
    print(f"Rows: {retailer_contact.shape[0]}")

    retailer_contact_code_sk = {}

    for index, row in retailer_contact.iterrows():
        query = 'INSERT INTO Retailer_contact (RETAILER_CONTACT_code, RETAILER_CONTACT_email, RETAILER_CONTACT_RETAILER_SITE_CODE_site_code, RETAILER_CONTACT_JOB_POSITION_EN_position, RETAILER_CONTACT_EXTENSION_extension, RETAILER_CONTACT_FAX_fax, RETAILER_CONTACT_FIRST_NAME_first_name, RETAILER_CONTACT_LAST_NAME_last_name) VALUES (?,?,?,?,?,?,?,?)'
        
        values = (row['RETAILER_CONTACT_code'], row['RETAILER_CONTACT_email'], retailer_site_code_sk_dict[row['RETAILER_CONTACT_RETAILER_SITE_CODE_site_code']], row['RETAILER_CONTACT_JOB_POSITION_EN_position'], row['RETAILER_CONTACT_EXTENSION_extension'], row['RETAILER_CONTACT_FAX_fax'], row['RETAILER_CONTACT_FIRST_NAME_first_name'], row['RETAILER_CONTACT_LAST_NAME_last_name'])
        
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())    
        retailer_contact_code_sk[row['RETAILER_CONTACT_code']] = generated_primary_key


    # ## retailer

    # 
    print(f"Rows: {retailer.shape[0]}")

    retailer_code_sk = {}

    for index, row in retailer.iterrows():
        query = 'INSERT INTO Retailer (RETAILER_code, RETAILER_name, RETAILER_COMPANY_CODE_MR_company, RETAILER_RETAILER_TYPE_code, RETAILER_RETAILER_TYPE_EN, CURRENT_VALUE) VALUES (?,?,?,?,?,?)'
        values = (row['RETAILER_code'], row['RETAILER_name'], row['RETAILER_COMPANY_CODE_MR_company'], row['RETAILER_RETAILER_TYPE_code'], row['RETAILER_RETAILER_TYPE_EN'], 1)
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        retailer_code_sk[row['RETAILER_code']] = generated_primary_key


    # ## product

    # 
    print(f"Rows: {product.shape[0]}")

    product_code_sk = {}

    for index, row in product.iterrows():
        query = 'INSERT INTO Product (PRODUCT_number, PRODUCT_name_product, PRODUCT_description_description, PRODUCT_image_image, PRODUCT_INTRODUCTION_DATE_introduced, PRODUCT_PRODUCTION_COST_cost, PRODUCT_MARGIN_margin, PRODUCT_LANGUAGE_language, PRODUCT_PRODUCT_LINE_code, PRODUCT_PRODUCT_LINE_code_en, PRODUCT_PRODUCT_TYPE_code, PRODUCT_PRODUCT_TYPE_code_en) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
        values = (row['PRODUCT_number'], row['PRODUCT_name_product'], row['PRODUCT_description_description'], row['PRODUCT_image_image'], row['PRODUCT_INTRODUCTION_DATE_introduced'], row['PRODUCT_PRODUCTION_COST_cost'], row['PRODUCT_MARGIN_margin'], row['PRODUCT_LANGUAGE_language'], row['PRODUCT_PRODUCT_LINE_code'], row['PRODUCT_PRODUCT_LINE_code_en'], row['PRODUCT_PRODUCT_TYPE_code'], row['PRODUCT_PRODUCT_TYPE_code_en'])
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        product_code_sk[row['PRODUCT_number']] = generated_primary_key


    # ## order_details

    # 
    print(f"Rows: {order_details.shape[0]}")

    order_detail_code_sk = {}

    for index, row in order_details.iterrows():
        query = 'INSERT INTO Order_details (ORDER_DETAILS_code, ORDER_DETAILS_QUANTITY_quantity, ORDER_DETAILS_TOTAL_COST_total, ORDER_DETAILS_TOTAL_MARGIN_margin, ORDER_DETAILS_RETURN_CODE_returned, ORDER_DETAILS_ORDER_NUMBER_order, ORDER_DETAILS_PRODUCT_NUMBER_product, ORDER_DETAILS_UNIT_ID_unit) VALUES (?,?,?,?,?,?,?,?)'
        values = (row['ORDER_DETAIL_code'], row['ORDER_DETAILS_QUANTITY_quantity'], row['ORDER_DETAILS_TOTAL_COST_total'], row['ORDER_DETAILS_TOTAL_MARGIN_margin'], row['ORDER_DETAILS_RETURN_CODE_returned'], row['ORDER_DETAILS_ORDER_NUMBER_order'], product_code_sk[row['ORDER_DETAILS_PRODUCT_NUMBER_product']], row['UNIT_id'])
        export_cursor.execute(query, values)
        
        # Execute the SELECT statement to retrieve the generated primary key
        query = 'SELECT @@IDENTITY AS GENERATED_KEY'
        export_cursor.execute(query)
        generated_primary_key = int(export_cursor.fetchval())
        order_detail_code_sk[row['ORDER_DETAIL_code']] = generated_primary_key
        


    # ## returned_item

    # 
    print(f"Rows: {returned_item.shape[0]}")
    for index, row in returned_item.iterrows():
        query = 'INSERT INTO Returned_item (RETURNED_ITEM_code, RETURNED_ITEM_DATE, RETURNED_ITEM_QUANTITY, RETURNED_ITEM_ORDER_DETAIL_CODE, RETURNED_ITEM_RETURN_REASON_code, RETURNED_ITEM_RETURN_REASON_description_en, RETURNED_ITEM_RETURNED_ITEMS_TOTAL_PRICE) VALUES (?,?,?,?,?,?,?)'
        values = (row['RETURNED_ITEM_code'], row['RETURNED_ITEM_DATE'], row['RETURNED_ITEM_QUANTITY'], order_detail_code_sk[row['RETURNED_ITEM_ORDER_DETAIL_CODE']], row['RETURNED_ITEM_RETURN_REASON_code'], row['RETURNED_ITEM_RETURN_REASON_description_en'], row['RETURNED_ITEM_RETURNED_ITEMS_TOTAL_PRICE'])
        export_cursor.execute(query, values)


    # ## sales_target_data

    # 
    print(f"Rows: {sales_target_data.shape[0]}")
    for index, row in sales_target_data.iterrows():
        query = 'INSERT INTO SALES_TARGETDATA (SALES_TARGETDATA_SALES_YEAR, SALES_TARGETDATA_SALES_PERIOD, SALES_TARGETDATA_RETAILER_NAME, SALES_TARGETDATA_SALES_TARGET, SALES_TARGETDATA_TARGET_COST, SALES_TARGETDATA_TARGET_MARGIN, SALES_TARGETDATA_SALES_STAFF_CODE, SALES_TARGETDATA_PRODUCT_NUMBER, SALES_TARGETDATA_RETAILER_CODE, SALES_TARGETDATA_SALES_TARGETDATA_ID) VALUES (?,?,?,?,?,?,?,?,?, ?)'
        values = (row['SALES_TARGETDATA_SALES_YEAR'], row['SALES_TARGETDATA_SALES_PERIOD'], row['SALES_TARGETDATA_RETAILER_NAME'], row['SALES_TARGETDATA_SALES_TARGET'], row['SALES_TARGET_DATA_TARGET_COST'], row['SALES_TARGET_DATA_TARGET_MARGIN'],  sales_staff_code_sk[row['SALES_TARGETDATA_SALES_STAFF_CODE']], product_code_sk[row['PRODUCT_number']], retailer_code_sk[row['RETAILER_code']], row['Id'])
        export_cursor.execute(query, values)


    # ## training

    # 
    print(f"Rows: {training.shape[0]}")
    for index, row in training.iterrows():
        query = 'INSERT INTO Training (TRAINING_SALES_STAFF_CODE, TRAINING_COURSE_CODE, TRAINING_YEAR) VALUES (?,?,?)'
        values = (sales_staff_code_sk[row['TRAINING_SALES_STAFF_CODE']], course_code_sk[row['TRAINING_COURSE_CODE']], row['TRAINING_YEAR'])
        export_cursor.execute(query, values)


    # ## satisfaction

    # 
    print(f"Rows: {satisfaction.shape[0]}")
    for index, row in satisfaction.iterrows():
        query = 'INSERT INTO Satisfaction (SATISFACTION_SALES_STAFF_CODE, SATISFACTION_SATISFACTION_TYPE_CODE, SATISFACTION_YEAR) VALUES (?,?,?)'
        values = (sales_staff_code_sk[row['SATISFACTION_SALES_STAFF_CODE']], satisfaction_type_code_sk[row['SATISFACTION_SATISFACTION_TYPE_CODE']], row['SATISFACTION_YEAR'])
        export_cursor.execute(query, values)


    # ## order_header

    # 
    print(f"Rows: {order_header.shape[0]}")

    for index, row in order_header.iterrows():
        query = 'INSERT INTO Order_header (ORDER_HEADER_number, ORDER_HEADER_SALES_STAFF_CODE, ORDER_HEADER_SALES_BRANCH_CODE, ORDER_HEADER_RETAILER_SITE_CODE, ORDER_HEADER_RETAILER_CONTACT_CODE, ORDER_HEADER_RETAILER_CODE, ORDER_HEADER_ORDER_DATE, ORDER_HEADER_ORDER_order_number) VALUES (?,?,?,?,?,?,?, ?)'
        values = (row['ORDER_HEADER_number'], sales_staff_code_sk[row['ORDER_HEADER_SALES_STAFF_CODE']], sales_branch_code_sk[row['ORDER_HEADER_SALES_BRANCH_CODE']], retailer_site_code_sk_dict[row['ORDER_HEADER_RETAILER_SITE_CODE']], retailer_contact_code_sk[row['ORDER_HEADER_RETAILER_CONTACT_CODE']], retailer_code_sk[row['RETAILER_CODE']], row['ORDER_HEADER_ORDER_DATE'], order_code_sk[row['ORDER_HEADER_number']])
        export_cursor.execute(query, values)


    # ## retailer_segment

    # 
    print(f"Rows: {retailer_segment.shape[0]}")
    for index, row in retailer_segment.iterrows():
        query = 'INSERT INTO Retailer_segment (RETAILER_SEGMENT_segment_code, RETAILER_SEGMENT_language, RETAILER_SEGMENT_segment_name, RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description, SEGMENT_DESCRIPTION_description_short, CURRENT_VALUE) VALUES (?,?,?,?,?,?)'
        values = (row['RETAILER_SEGMENT_segment_code'], row['RETAILER_SEGMENT_language'], row['RETAILER_SEGMENT_segment_name'], row['RETAILER_SEGMENT_SEGMENT_DESCRIPTION_description'], row['SEGMENT_DESCRIPTION_SHORT'], 1)
        export_cursor.execute(query, values)


    # ## retailer_headquarters

    # 
    print(f"Rows: {retailer_headquarters.shape[0]}")

    for index, row in retailer_headquarters.iterrows():
        query = 'INSERT INTO Retailer_headquarter (RETAILER_HEADQUARTER_codemr, RETAIL_HEADQUARTER_retailer_name, RETAILER_HEADQUARTER_address1_address, RETAILER_HEADQUARTER_address2_address, RETAILER_HEADQUARTER_city_city, RETAILER_HEADQUARTER_region_region, RETAILER_HEADQUARTER_postal_zone_postal_zone, RETAILER_HEADQUARTER_country_code_country, RETAILER_HEADQUARTER_phone_phone, RETAILER_HEADQUARTER_fax_fax, RETAILER_HEADQUARTER_segment_code, RETAILER_HEADQUARTER_main_address_address) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
        values = (row['RETAILER_HEADQUARTER_codemr'], row['RETAIL_HEADQUARTER_retailer_name'], row['RETAILER_HEADQUARTER_address1_address'], row['RETAILER_HEADQUARTER_address2_address'], row['RETAILER_HEADQUARTER_city_city'], row['RETAILER_HEADQUARTER_region_region'], row['RETAILER_HEADQUARTER_postal_zone_postal_zone'], row['RETAILER_HEADQUARTER_country_code_country'], row['RETAILER_HEADQUARTER_phone_phone'], row['RETAILER_HEADQUARTER_fax_fax'], row['RETAILER_HEADQUARTER_segment_code'], row['MAIN_ADDRESS'])
        export_cursor.execute(query, values)


    # ## Go_sales_inventory_levels

    # 
    print(f"Rows: {go_sales_inventory_levels.shape[0]}")
    for index, row in go_sales_inventory_levels.iterrows():
        query = 'INSERT INTO GO_SALES_INVENTORY_LEVELS (GO_SALES_INVENTORY_LEVELS_PRODUCT_NUMBER, GO_SALES_INVENTORY_LEVELS_YEAR_MONTH, GO_SALES_INVENTORY_LEVELS_INVENTORY_COUNT, GO_SALES_INVENTORY_LEVELS_id) VALUES (?,?,?, ?)'
        values = (product_code_sk[row['GO_SALES_INVENTORY_LEVELS_PRODUCT_NUMBER']], row['GO_SALES_INVENTORY_LEVELS_YEAR_MONTH'], row['GO_SALES_INVENTORY_LEVELS_INVENTORY_COUNT'], index)
        export_cursor.execute(query, values)


    # ## Go_sales_product_forecast

    # 
    print(f"Rows: {sales_product_forecast.shape[0]}")

    # converting expected cost and expected margin to float
    sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_COST'] = sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_COST'].astype('float')
    sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN'] = sales_product_forecast['GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN'].astype('float')

    for index, row in sales_product_forecast.iterrows():
        query = 'INSERT INTO GO_SALES_PRODUCT_FORECAST (GO_SALES_PRODUCT_FORECAST_PRODUCT_NUMBER, GO_SALES_PRODUCT_FORECAST_YEAR_MONTH, GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME, GO_SALES_PRODUCT_FORECAST_EXPECTED_COST, GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN, GO_SALES_PRODUCT_FORECAST_id) VALUES (?,?,?,?,?,?)'
        values = (product_code_sk[row['GO_SALES_PRODUCT_FORECAST_PRODUCT_NUMBER']], row['GO_SALES_PRODUCT_FORECAST_YEAR_MONTH'],row['GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME'], row['GO_SALES_PRODUCT_FORECAST_EXPECTED_COST'], row['GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN'], index)
        export_cursor.execute(query, values)


    # ## clean up

    # 
    # close cursor
    export_cursor.close()

    # close connection
    export_conn.close()



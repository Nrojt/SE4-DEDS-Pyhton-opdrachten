CREATE DATABASE DEDS_DataWarehouse;

CREATE TABLE Returned_item (
    Returned_item INT PRIMARY KEY,
    RETURNED_ITEM_code VARCHAR(20) NOT NULL,
    RETURNED_ITEM_DATE DATETIME NOT NULL,
    RETURNED_ITEM_QUANTITY INT NOT NULL,
    RETURNED_ITEM_ORDER_DETAIL_CODE INT REFERENCES Order_Details(ORDER_DETAILs_code),
    RETURNED_ITEM_RETURN_REASON_code INT NOT NULL,
    RETURNED_ITEM_RETURN_REASON_desctiption_en VARCHAR(255),
    RETURNED_ITEM_RETURNED_ITEMS_TOTAL_PRICE DECIMAL(10, 2) NOT NULL
);

CREATE TABLE Unit (
  UNIT_id int PRIMARY KEY,
  UNIT_COST_cost money NOT NULL,
  UNIT_PRICE_price money NOT NULL,
  UNIT_SALE_sale bit NOT NULL
);

CREATE TABLE Sales_staff (
  SALES_STAFF_code int PRIMARY KEY,
  SALES_STAFF_email varchar(255) NOT NULL,
  SALES_STAFF_extension varchar(50) NOT NULL,
  SALES_STAFF_POSITION_EN_position varchar(50) NOT NULL,
  SALES_STAFF_WORK_PHONE_work_phone varchar(50) NOT NULL,
  SALES_STAFF_DATE_HIRED_hired date NOT NULL,
  SALES_STAFF_MANAGER_CODE_manager int,
  SALES_STAFF_FAX varchar(50) NOT NULL,
  SALES_STAFF_FIRST_NAME_first_name varchar(50) NOT NULL,
  SALES_STAFF_LAST_NAME_last_name varchar(50) NOT NULL,
  SALES_STAFF_FULL_NAME__full_name varchar(255), -- Consider using a calculated field for full name
  SALES_STAFF_SALES_BRANCH_CODE_branch_code int,
  SALES_STAFF_SALES_BRANCH_ADDRESS_address varchar(255),
  SALES_STAFF_SALES_BRANCH_ADDRESS_EXTRA_address_extra varchar(255)
);

CREATE TABLE Satisfaction_type (
  SATISFACTION_TYPE_code int PRIMARY KEY,
  SATISFACTION_TYPE_description nvarchar(255) NOT NULL,
  SATISFACTION_TYPE_description_short nvarchar(50) NOT NULL
);

CREATE TABLE Course (
  COURSE_code int PRIMARY KEY,
  COURSE_description nvarchar(max) NOT NULL, -- Consider using a narrower data type if description length is limited
  COURSE_description_short nvarchar(255) NOT NULL
);

CREATE TABLE Year (
  Year int PRIMARY KEY,
  UNIQUE (Year), -- Add UNIQUE constraint for explicit enforcement
  CHECK (Year >= 1000 AND Year <= 9999) -- Adjust year range as needed
);

CREATE TABLE Date (
  DATE_date date PRIMARY KEY -- Store the complete date in a single column
);

CREATE TABLE Order (
  ORDER_order_number int PRIMARY KEY,
  ORDER_ORDER_METHOD_CODE_method_code int,
  ORDER_ORDER_METHOD_EN_method nvarchar(50) NOT NULL,
);

CREATE TABLE Retailer_site (
  RETAILER_SITE_code int PRIMARY KEY,
  RETAILER_SITE_COUNTRY_CODE_country varchar(50) NOT NULL,
  RETAILER_SITE_CITY_city varchar(50) NOT NULL,
  RETAILER_SITE_REGION_region varchar(50),
  RETAILER_SITE_POSTAL_ZONE_postal_zone varchar(20),
  RETAILER_SITE_RETAILER_CODE_retailer_code int,
  RETAILER_SITE_ACTIVE_INDICATOR_indicator bit NOT NULL,
  RETAILER_SITE_ADDRESS1_address varchar(255),
  RETAILER_SITE_ADDRESS2_address varchar(255),
  RETAILER_SITE_ADDRESS3_address varchar(255),
  -- Consider using a calculated field for full address instead of storing it separately
  RETAILER_SITE_MAIN_ADDRESS__address nvarchar(max)
);

CREATE TABLE Sales_branch (
  SALES_BRANCH_code int PRIMARY KEY,
  SALES_BRANCH_COUNTRY_CODE_country varchar(50) NOT NULL,
  SALES_BRANCH_REGION_region varchar(50),
  SALES_BRANCH_CITY_city varchar(50) NOT NULL,
  SALES_BRANCH_POSTAL_ZONE_postal_zone varchar(20),
  SALES_BRANCH_ADDRESS1_address varchar(255),
  SALES_BRANCH_ADDRESS2_address varchar(255),
  -- Consider using a calculated field for full address instead of storing it separately
  SALES_BRANCH_MAIN_ADDRESS_address nvarchar(max)
);

CREATE TABLE Retailer_contact (
  RETAILER_CONTACT_code int PRIMARY KEY,
  RETAILER_CONTACT_email varchar(255) NOT NULL,
  RETAILER_CONTACT_RETAILER_SITE_CODE_site_code int,
  FOREIGN KEY (RETAILER_CONTACT_RETAILER_SITE_CODE_site_code) REFERENCES Retailer_site(RETAILER_SITE_code),
  RETAILER_CONTACT_JOB_POSITION_EN_position varchar(50),
  RETAILER_CONTACT_EXTENSION_extension varchar(50),
  RETAILER_CONTACT_FAX_fax varchar(50),
  RETAILER_CONTACT_GENDER_gender varchar(10), -- Consider using a narrower data type if gender is limited to specific options
  RETAILER_CONTACT_FIRST_NAME_first_name varchar(50) NOT NULL,
  RETAILER_CONTACT_LAST_NAME_last_name varchar(50) NOT NULL,
  -- Consider using a calculated field for full name instead of storing it separately
  RETAILER_CONTACT_FULL_NAME_full_name nvarchar(255)
);

CREATE TABLE Retailer (
  RETAILER_code int PRIMARY KEY,
  RETAILER_name nvarchar(255) NOT NULL,
  RETAILER_COMPANY_CODE_MR_company varchar(50), -- Assuming COMPANY_CODE_MR refers to company code in Marketing Research system
  RETAILER_RETAILER_TYPE_code int,
  RETAILER_RETAILER_TYPE_EN nvarchar(50) NOT NULL,
);

CREATE TABLE Product (
  PRODUCT_number INT PRIMARY KEY,
  PRODUCT_name_product NVARCHAR(MAX) NOT NULL,
  PRODUCT_description_description NVARCHAR(MAX),
  PRODUCT_image_image VARBINARY(MAX),
  PRODUCT_INTRODUCTION_DATE_introduced DATE,
  PRODUCT_PRODUCTION_COST_cost DECIMAL(10, 2) NOT NULL,
  PRODUCT_MARGIN_margin DECIMAL(5, 2) NOT NULL,
  PRODUCT_LANGUAGE_language NVARCHAR(5) NOT NULL,
  PRODUCT_MINIMUM_SALE_PRICE_minPrice DECIMAL(10, 2) NOT NULL,
  PRODUCT_PRODUCT_LINE_code INT,
  PRODUCT_PRODUCT_LINE_code_en NVARCHAR(5),
  PRODUCT_PRODUCT_TYPE_code INT,
  PRODUCT_PRODUCT_TYPE_code_en NVARCHAR(5)
);


CREATE TABLE Order_details (
  ORDER_DETAILS_code int PRIMARY KEY,
  ORDER_DETAILS_QUANTITY_quantity int NOT NULL,
  ORDER_DETAILS_TOTAL_COST_total money NOT NULL,
  ORDER_DETAILS_TOTAL_MARGIN_margin money NOT NULL,
  ORDER_DETAILS_RETURN_CODE_returned varchar(50),
  FOREIGN KEY (ORDER_DETAILS_ORDER_NUMBER_order) REFERENCES Order(ORDER_order_number),
  FOREIGN KEY (ORDER_DETAILS_PRODUCT_NUMBER_product) REFERENCES Product(PRODUCT_number),
  FOREIGN KEY (ORDER_DETAILS_UNIT_ID_unit) REFERENCES Unit(UNIT_id)
);

CREATE TABLE SALES_TARGETDATA (
  SALES_TARGETDATA_id int PRIMARY KEY,
  SALES_TARGETDATA_SALES_YEAR int NOT NULL,
  SALES_TARGETDATA_SALES_PERIOD varchar(50) NOT NULL,
  SALES_TARGETDATA_RETAILER_NAME nvarchar(255),
  SALES_TARGETDATA_SALES_TARGET money NOT NULL,
  SALES_TARGETDATA_TARGET_COST money NOT NULL,
  SALES_TARGETDATA_TARGET_MARGIN numeric(5,2) NOT NULL,
  FOREIGN KEY (SALES_TARGETDATA_SALES_STAFF_CODE) REFERENCES Sales_staff(SALES_STAFF_code),
  FOREIGN KEY (SALES_TARGETDATA_PRODUCT_NUMBER) REFERENCES Product(PRODUCT_number),
  FOREIGN KEY (SALES_TARGETDATA_RETAILER_CODE) REFERENCES Retailer(RETAILER_code)
);

CREATE TABLE Training (
  TRAINING_id INT PRIMARY KEY IDENTITY,  -- Use auto-incrementing ID as primary key
  TRAINING_SALES_STAFF_CODE INT NOT NULL,
  FOREIGN KEY (TRAINING_SALES_STAFF_CODE) REFERENCES Sales_staff(SALES_STAFF_code),
  TRAINING_COURSE_CODE INT NOT NULL,
  FOREIGN KEY (TRAINING_COURSE_CODE) REFERENCES Training_course(TRAINING_COURSE_code),
  TRAINING_START_DATE DATE,  -- Include start date for tracking completion or ongoing training
  TRAINING_END_DATE DATE,    -- Include end date for tracking completion or duration
  TRAINING_STATUS VARCHAR(10) CHECK (TRAINING_STATUS IN ('Enrolled', 'Completed', 'Cancelled')),  -- Add status for completion tracking
  UNIQUE (TRAINING_SALES_STAFF_CODE, TRAINING_COURSE_CODE, TRAINING_START_DATE)  -- Enforce unique combination for same staff, course, and start date
);

CREATE TABLE Satisfaction (
  SATISFACTION_id INT PRIMARY KEY IDENTITY,  -- Use auto-incrementing ID as primary key -- Use auto-incrementing ID as primary key
  SATISFACTION_SALES_STAFF_CODE INT NOT NULL,
  FOREIGN KEY (SATISFACTION_SALES_STAFF_CODE) REFERENCES Sales_staff(SALES_STAFF_code),
  SATISFACTION_SATISFACTION_TYPE_CODE INT NOT NULL,
  FOREIGN KEY (SATISFACTION_SATISFACTION_TYPE_CODE) REFERENCES Satisfaction_type(SATISFACTION_TYPE_code),
  SATISFACTION_YEAR INT NOT NULL,
  SATISFACTION_VALUE numeric(5,2) NOT NULL  -- Assuming satisfaction is numerical with a scale of 2 decimal places
);

CREATE TABLE Order_header (
  ORDER_HEADER_number int PRIMARY KEY,  -- Unique order identifier
  ORDER_HEADER_RETAILER_CODE int NOT NULL,  -- Foreign key referencing Retailer table
  FOREIGN KEY (ORDER_HEADER_RETAILER_CODE) REFERENCES Retailer(RETAILER_code),
  ORDER_HEADER_SALES_STAFF_CODE int NOT NULL,  -- Foreign key referencing Sales_staff table
  FOREIGN KEY (ORDER_HEADER_SALES_STAFF_CODE) REFERENCES Sales_staff(SALES_STAFF_code),
  ORDER_HEADER_SALES_BRANCH_CODE int,  -- Foreign key referencing Sales_branch table (nullable)
  FOREIGN KEY (ORDER_HEADER_SALES_BRANCH_CODE) REFERENCES Sales_branch(SALES_BRANCH_code),
  ORDER_HEADER_ORDER_DATE date NOT NULL,
  ORDER_HEADER_SHIP_DATE date, -- Add optional shipping date
  ORDER_HEADER_STATUS VARCHAR(10) CHECK( ORDER_HEADER_STATUS IN('New', 'Processing', 'Shipped', 'Cancelled')),  -- Track order status
  ORDER_HEADER_RETAILER_SITE_CODE int,  -- Foreign key referencing Retailer_site table (nullable)
  FOREIGN KEY (ORDER_HEADER_RETAILER_SITE_CODE) REFERENCES Retailer_site(RETAILER_SITE_code),
  ORDER_HEADER_RETAILER_CONTACT_CODE int,  -- Foreign key referencing Retailer_contact table (nullable)
  FOREIGN KEY (ORDER_HEADER_RETAILER_CONTACT_CODE) REFERENCES Retailer_contact(RETAILER_CONTACT_code)
);

CREATE TABLE GO_SALES_INVENTORY_LEVELS (
  GO_SALES_INVENTORY_LEVELS_id INT PRIMARY KEY IDENTITY, -- Use auto-incrementing ID as primary key
  GO_SALES_INVENTORY_LEVELS_INVENTORY_COUNT INT NOT NULL,
  GO_SALES_INVENTORY_LEVELS_PRODUCT_NUMBER INT NOT NULL,
  FOREIGN KEY (GO_SALES_INVENTORY_LEVELS_PRODUCT_NUMBER) REFERENCES Product (PRODUCT_number),
  GO_SALES_INVENTORY_LEVELS_YEAR_MONTH VARCHAR(10) NOT NULL,  -- Combine year and month for efficient storage and retrieval
  CHECK (LEN(GO_SALES_INVENTORY_LEVELS_YEAR_MONTH) = 6)  -- Enforce year-month format (YYYYMM)
);

CREATE TABLE GO_SALES_PRODUCT_FORECAST (
  GO_SALES_PRODUCT_FORECAST_id INT PRIMARY KEY IDENTITY, -- Use auto-incrementing ID as primary key
  GO_SALES_PRODUCT_FORECAST_EXPECTED_VOLUME INT NOT NULL,
  GO_SALES_PRODUCT_FORECAST_EXPECTED_COST MONEY NOT NULL,
  GO_SALES_PRODUCT_FORECAST_EXPECTED_MARGIN NUMERIC(5,2) NOT NULL,
  FOREIGN KEY (GO_SALES_PRODUCT_FORECAST_PRODUCT_NUMBER) REFERENCES Product(PRODUCT_number),
  GO_SALES_PRODUCT_FORECAST_YEAR_MONTH VARCHAR(10) NOT NULL, -- Combine year and month for efficient storage and retrieval
  CHECK (LEN(GO_SALES_PRODUCT_FORECAST_YEAR_MONTH) = 6) -- Enforce year-month format (YYYYMM)
);

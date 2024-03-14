USE DEDS_DataWarehouse;
GO

-- triggers
CREATE TRIGGER trg_Unit_CurrentValue
ON Unit
AFTER INSERT, UPDATE
AS
BEGIN
    IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
    BEGIN
        DECLARE @unit_sk int = (SELECT UNIT_SK FROM inserted);
        UPDATE Unit
        SET CURRENT_VALUE = 0
        WHERE UNIT_SK <> @unit_sk;
    END
END;
GO

CREATE TRIGGER trg_Sales_staff_CurrentValue
ON Sales_staff
AFTER INSERT, UPDATE
AS
BEGIN
    IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
    BEGIN
        DECLARE @sales_staff_sk int = (SELECT SALES_STAFF_SK FROM inserted);
        UPDATE Sales_staff
        SET CURRENT_VALUE = 0
        WHERE SALES_STAFF_SK <> @sales_staff_sk;
    END
END;
GO

CREATE TRIGGER trg_Satisfaction_type_CurrentValue
ON Satisfaction_type
AFTER INSERT, UPDATE
AS
BEGIN
    IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
    BEGIN
        DECLARE @unit_sk int = (SELECT SATISFACTION_TYPE_SK FROM inserted);
        UPDATE Satisfaction_type
        SET CURRENT_VALUE = 0
        WHERE SATISFACTION_TYPE_SK <> @unit_sk;
    END
END;
GO

CREATE TRIGGER trg_Course_CurrentValue
ON Course
AFTER INSERT, UPDATE
AS
BEGIN
    IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
    BEGIN
        DECLARE @unit_sk int = (SELECT COURSE_SK FROM inserted);
        UPDATE COURSE
        SET CURRENT_VALUE = 0
        WHERE COURSE_SK <> @unit_sk;
    END
END;
GO

CREATE TRIGGER trg_Order_CurrentValue
    ON "Order"
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT ORDER_SK FROM inserted);
            UPDATE "Order"
            SET CURRENT_VALUE = 0
            WHERE ORDER_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Retailer_site_CurrentValue
    ON Retailer_site
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT RETAILER_SITE_SK FROM inserted);
            UPDATE Retailer_site
            SET CURRENT_VALUE = 0
            WHERE RETAILER_SITE_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Retailer_segment_CurrentValue
    ON Retailer_segment
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT RETAILER_SEGMENT_SK FROM inserted);
            UPDATE Retailer_segment
            SET CURRENT_VALUE = 0
            WHERE RETAILER_SEGMENT_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Retailer_headquarter_CurrentValue
    ON Retailer_headquarter
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT RETAILER_HEADQUARTER_SK FROM inserted);
            UPDATE Retailer_headquarter
            SET CURRENT_VALUE = 0
            WHERE RETAILER_HEADQUARTER_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Sales_branch_CurrentValue
    ON Sales_branch
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT SALE_BRANCH_SK FROM inserted);
            UPDATE Sales_branch
            SET CURRENT_VALUE = 0
            WHERE SALE_BRANCH_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Retailer_contact_CurrentValue
    ON Retailer_contact
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT RETAILER_CONTACT_SK FROM inserted);
            UPDATE Retailer_contact
            SET CURRENT_VALUE = 0
            WHERE RETAILER_CONTACT_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Retailer_CurrentValue
    ON Retailer
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT RETAILER_SK FROM inserted);
            UPDATE Retailer
            SET CURRENT_VALUE = 0
            WHERE RETAILER_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Product_CurrentValue
    ON Product
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT PRODUCT_SK FROM inserted);
            UPDATE Product
            SET CURRENT_VALUE = 0
            WHERE PRODUCT_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Order_details_CurrentValue
    ON Order_details
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT ORDER_DETAILS_SK FROM inserted);
            UPDATE Order_details
            SET CURRENT_VALUE = 0
            WHERE ORDER_DETAILS_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Returned_item_CurrentValue
    ON Returned_item
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT RETURNED_ITEM_SK FROM inserted);
            UPDATE Returned_item
            SET CURRENT_VALUE = 0
            WHERE RETURNED_ITEM_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_SALES_TARGETDATA_CurrentValue
    ON SALES_TARGETDATA
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT SALES_TARGETDATA_SK FROM inserted);
            UPDATE SALES_TARGETDATA
            SET CURRENT_VALUE = 0
            WHERE SALES_TARGETDATA_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Training_CurrentValue
    ON Training
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT TRAINING_SALES_STAFF_CODE FROM inserted);
            UPDATE Training
            SET CURRENT_VALUE = 0
            WHERE TRAINING_SALES_STAFF_CODE <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Satisfaction_CurrentValue
    ON Satisfaction
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT SATISFACTION_SALES_STAFF_CODE FROM inserted);
            UPDATE Satisfaction
            SET CURRENT_VALUE = 0
            WHERE SATISFACTION_SALES_STAFF_CODE <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_Order_header_CurrentValue
    ON Order_header
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT ORDER_HEADER_SK FROM inserted);
            UPDATE Order_header
            SET CURRENT_VALUE = 0
            WHERE ORDER_HEADER_SK <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_GO_SALES_INVENTORY_LEVELS_CurrentValue
    ON GO_SALES_INVENTORY_LEVELS
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT GO_SALES_INVENTORY_LEVELS_id FROM inserted);
            UPDATE GO_SALES_INVENTORY_LEVELS
            SET CURRENT_VALUE = 0
            WHERE GO_SALES_INVENTORY_LEVELS_id <> @unit_sk;
        END
    END;
GO

CREATE TRIGGER trg_GO_SALES_PRODUCT_FORECAST_CurrentValue
    ON GO_SALES_PRODUCT_FORECAST
    AFTER INSERT, UPDATE
AS
    BEGIN
        IF EXISTS (SELECT 1 FROM inserted WHERE CURRENT_VALUE = 1)
        BEGIN
            DECLARE @unit_sk int = (SELECT GO_SALES_PRODUCT_FORECAST_id FROM inserted);
            UPDATE GO_SALES_PRODUCT_FORECAST
            SET CURRENT_VALUE = 0
            WHERE GO_SALES_PRODUCT_FORECAST_id <> @unit_sk;
        END
    END;

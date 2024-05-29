CREATE TABLE table_m3 (
    "Customer ID" INT PRIMARY KEY,
    "Age" INT,
    "Gender" VARCHAR(10),
    "Item Purchased" VARCHAR(100),
    Category VARCHAR(50),
    "Purchase Amount (USD)" FLOAT,
    "Location" VARCHAR(100),
    "Size" VARCHAR(10),
    "Color" VARCHAR(20),
    "Season" VARCHAR(20),
    "Review Rating" FLOAT,
    "Subscription Status" VARCHAR(3),
    "Shipping Type" VARCHAR(20),
    "Discount Applied" VARCHAR(3),
    "Promo Code Used" VARCHAR(3),
    "Previous Purchases" INT,
    "Payment Method" VARCHAR(50),
    "Frequency of Purchases" VARCHAR(20)
);

Copy table_m3
From 'C:\Program Files\PostgreSQL\16\P2M3_Alfarabi_data_raw.csv.csv'
DELIMITER','
CSV HEADER;

Select * From table_m3
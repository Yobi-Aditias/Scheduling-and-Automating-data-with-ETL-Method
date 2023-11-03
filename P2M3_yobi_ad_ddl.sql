-- Buat database
CREATE DATABASE milestone3;

-- Buat tabel
CREATE TABLE table_m3(
	"ID" INTEGER, 
	"Year_Birth" INTEGER, 
	"Education" VARCHAR(100), 
	"Marital_Status" VARCHAR(100), 
	"Income" NUMERIC(7), 
	"Kidhome" INTEGER,
    "Teenhome" INTEGER, 
	"Dt_Customer" VARCHAR(100), --karena ditugaskan untuk menyamakan persis dengan file dari sumber
	"Recency" INTEGER, --maka kolom Dt_Customer tetap Varchar karena format tidak sesuai dengan format dtsehingga akan dibersihkan di proses cleanng data
	"MntWines" INTEGER, 
	"MntFruits" INTEGER,
    "MntMeatProducts" INTEGER, 
	"MntFishProducts" INTEGER, 
	"MntSweetProducts" INTEGER,
    "MntGoldProds" INTEGER, 
	"NumDealsPurchases" INTEGER, 
	"NumWebPurchases" INTEGER,
    "NumCatalogPurchases" INTEGER, 
	"NumStorePurchases" INTEGER, 
	"NumWebVisitsMonth" INTEGER,
    "AcceptedCmp3" INTEGER, 
	"AcceptedCmp4" INTEGER, 
	"AcceptedCmp5" INTEGER, 
	"AcceptedCmp1" INTEGER,
    "AcceptedCmp2" INTEGER, 
	"Complain" INTEGER, 
	"Z_CostContact" INTEGER, 
	"Z_Revenue" INTEGER, 
	"Response" INTEGER
);

-- Import data dari file CSV
COPY table_m3
FROM 'C:\BootcampHacktiv8\Milestone\Milestone_Phase_2\marketing_campaign.csv'
DELIMITER E'\t'
CSV HEADER;

SELECT * FROM table_m3;
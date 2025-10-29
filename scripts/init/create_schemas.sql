/*
=============================================================
Create Schemas
=============================================================

File: scripts/init/create_schemas.sql

Script Purpose: Create core DW schemas (bronze, silver, gold)

Run Context: connected to database 'datawarehouse'
*/


-- Create Bronze Schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create Silver Schema
CREATE SCHEMA IF NOT EXISTS silver;

-- Create Gold Schema
CREATE SCHEMA IF NOT EXISTS gold;





/*
=============================================================
Create Schemas
=============================================================

File: scripts/ddl/01_create_schemas.sql

Script Purpose: Create core DW schemas (bronze, silver, gold)

Run Context: connected to database 'datawarehouse'
*/





CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;





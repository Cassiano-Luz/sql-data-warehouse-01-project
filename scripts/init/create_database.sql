/*
=============================================================
Create Database
=============================================================

File: scripts/init/create_database.sql

Script Purpose:

	- This script creates a new database named 'DataWarehouse' after checking if it already exists.
	- If the database exists, it is dropped and recreated.

Run Context: connected to database 'postgres'.

WARNING:
    - Running this script will drop the entire 'DataWarehouse' database if it exists. 
    - All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
	- Requires PostgreSQL 13+ for DROP DATABASE ... WITH (FORCE).
	- No OWNER specified to keep the script portable (creator becomes owner).
*/





-- Drop and recreate theASE IF EXISTS "DataWarehouse" (Run connected to database: postgres);
DROP DATABASE IF EXISTS DataWarehouse WITH (FORCE);

CREATE DATABASE DataWarehouse
ENCODING 'UTF8'
TEMPLATE template0;





-- Database: Data_Warehouse_Project

-- DROP DATABASE IF EXISTS "Data_Warehouse_Project";

CREATE DATABASE "Data_Warehouse_Project"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

COMMENT ON DATABASE "Data_Warehouse_Project"
    IS 'A project to create Data Warehouse from scratch';

Create schema Bronze;

Create schema Silver;

Create schema Gold;
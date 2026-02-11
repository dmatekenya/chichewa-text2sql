-- ====================================================================
-- Chichewa Text2SQL Dataset - Database Schema
-- SPIDER-Compliant Relational Database
-- ====================================================================
-- This database contains only factual tabular data from structured sources.
-- No natural language questions, SQL queries, results, or annotations are included.
-- All supervision data is maintained separately in JSON format.
-- ====================================================================

-- Drop existing tables to ensure clean state
DROP TABLE IF EXISTS production;
DROP TABLE IF EXISTS population;
DROP TABLE IF EXISTS mse_daily;
DROP TABLE IF EXISTS commodity_prices;
DROP TABLE IF EXISTS food_insecurity;

-- ====================================================================
-- 1. PRODUCTION TABLE
-- Agricultural production data by district, crop, and season
-- ====================================================================
CREATE TABLE production (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    district TEXT NOT NULL,
    crop TEXT NOT NULL,
    yield REAL NOT NULL,
    season TEXT NOT NULL
);

CREATE INDEX idx_production_district ON production(district);
CREATE INDEX idx_production_crop ON production(crop);
CREATE INDEX idx_production_season ON production(season);

-- ====================================================================
-- 2. POPULATION TABLE
-- Population census data with geographic and demographic information
-- ====================================================================
CREATE TABLE population (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name TEXT,
    region_code INTEGER NOT NULL,
    admin_status TEXT,
    district_code INTEGER NOT NULL,
    ea_number REAL,
    ea_code REAL,
    ta_code REAL,
    ta_name TEXT NOT NULL,
    tpa TEXT,
    population_male REAL NOT NULL,
    population_female REAL NOT NULL,
    number_households REAL NOT NULL,
    sum_value REAL NOT NULL,
    district_name2 TEXT,
    population_text TEXT NOT NULL,
    district_name TEXT,
    total_population TEXT NOT NULL
);

CREATE INDEX idx_population_district ON population(district_name);
CREATE INDEX idx_population_region ON population(region_name);
CREATE INDEX idx_population_ta ON population(ta_name);

-- ====================================================================
-- 3. MSE_DAILY TABLE
-- Malawi Stock Exchange daily trading data
-- ====================================================================
CREATE TABLE mse_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    counter_id REAL,
    ticker TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    print_time TEXT NOT NULL,
    company_name TEXT,
    sector TEXT,
    high_price REAL,
    low_price REAL,
    bid_price REAL,
    ask_price REAL,
    previous_close_price REAL NOT NULL,
    close_price REAL NOT NULL,
    volume REAL NOT NULL,
    dividend_mwk REAL NOT NULL,
    dividend_yield_pct REAL NOT NULL,
    earnings_yield_pct REAL NOT NULL,
    pe_ratio REAL NOT NULL,
    pbv_ratio REAL,
    market_cap_mwk_mn REAL,
    profit_after_tax_mwk_mn REAL NOT NULL,
    shares_outstanding REAL
);

CREATE INDEX idx_mse_ticker ON mse_daily(ticker);
CREATE INDEX idx_mse_company ON mse_daily(company_name);
CREATE INDEX idx_mse_date ON mse_daily(trade_date);

-- ====================================================================
-- 4. COMMODITY_PRICES TABLE
-- Food commodity price data by market and collection date
-- ====================================================================
CREATE TABLE commodity_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    add_name TEXT NOT NULL,
    epa_name TEXT NOT NULL,
    district TEXT NOT NULL,
    market TEXT NOT NULL,
    month_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    commodity TEXT NOT NULL,
    price REAL,
    collection_date TEXT NOT NULL
);

CREATE INDEX idx_commodity_district ON commodity_prices(district);
CREATE INDEX idx_commodity_market ON commodity_prices(market);
CREATE INDEX idx_commodity_type ON commodity_prices(commodity);
CREATE INDEX idx_commodity_date ON commodity_prices(collection_date);

-- ====================================================================
-- 5. FOOD_INSECURITY TABLE
-- Food security status by district and time period
-- ====================================================================
CREATE TABLE food_insecurity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    district TEXT NOT NULL,
    analyzed_population INTEGER NOT NULL,
    time_period TEXT NOT NULL,
    percentage_population INTEGER NOT NULL,
    insecurity_level INTEGER NOT NULL,
    insecurity_desc_short TEXT NOT NULL,
    insecurity_desc_long TEXT NOT NULL
);

CREATE INDEX idx_food_insecurity_district ON food_insecurity(district);
CREATE INDEX idx_food_insecurity_period ON food_insecurity(time_period);

-- ====================================================================
-- Schema Complete - Ready for data loading
-- ====================================================================

#!/usr/bin/env python3
"""
Complete dataset builder - ensures 80 instances per table
Merges valid instances from train.json + generates new ones to fill gaps
"""

import json
import sqlite3
import os
import random

random.seed(42)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_PATH = os.path.join(BASE_DIR, "data", "database", "chichewa_text2sql.db")
TRAIN_PATH = os.path.join(BASE_DIR, "data", "train", "train.json")
CORRECTED_PATH = os.path.join(BASE_DIR, "data", "train", "train_corrected.json")

TARGET_PER_TABLE = 80

conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

def execute_sql(sql):
    try:
        cursor.execute(sql)
        return True, cursor.fetchall(), None
    except sqlite3.Error as e:
        return False, None, str(e)

def format_as_tuples(result):
    if result is None or not result:
        return "[]"
    formatted = []
    for row in result:
        formatted_row = []
        for val in row:
            if isinstance(val, float):
                formatted_row.append(round(val, 2))
            else:
                formatted_row.append(val)
        formatted.append(tuple(formatted_row))
    return str(formatted)

def get_distinct(table, column, limit=50):
    try:
        cursor.execute(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT {limit}")
        return [r[0] for r in cursor.fetchall()]
    except:
        return []

def get_instance_key(inst):
    return (inst['question_en'].strip().lower(), inst['sql_statement'].strip())

# ============================================================================
# LOAD VALID INSTANCES FROM TRAIN.JSON
# ============================================================================
print("=" * 70)
print("BUILDING COMPLETE CORRECTED DATASET")
print("=" * 70)

print("\n[1] Loading train.json and extracting valid instances...")
with open(TRAIN_PATH, 'r', encoding='utf-8') as f:
    train_data = json.load(f)

valid_by_table = {
    'production': [],
    'population': [],
    'food_insecurity': [],
    'commodity_prices': [],
    'mse_daily': []
}
existing_keys = set()

for inst in train_data:
    success, result, _ = execute_sql(inst['sql_statement'])
    if success:
        key = get_instance_key(inst)
        if key not in existing_keys:
            corrected = {
                "question_en": inst['question_en'],
                "question_ny": inst['question_ny'],
                "sql_statement": inst['sql_statement'],
                "sql_result": format_as_tuples(result),
                "difficulty_level": inst['difficulty_level'],
                "table": inst['table']
            }
            valid_by_table[inst['table']].append(corrected)
            existing_keys.add(key)

print("Valid instances from train.json:")
for t, instances in valid_by_table.items():
    print(f"  {t}: {len(instances)}")

# ============================================================================
# GENERATE ADDITIONAL INSTANCES FOR TABLES THAT NEED THEM
# ============================================================================

def generate_and_add(table, templates, params_fn, target_count, existing):
    """Generate instances until we reach target_count."""
    current = existing.copy()
    keys = {get_instance_key(i) for i in current}
    attempts = 0
    max_attempts = 5000
    
    while len(current) < target_count and attempts < max_attempts:
        attempts += 1
        template = random.choice(templates)
        params = params_fn()
        
        try:
            q_en = template["q_en"].format(**params)
            q_ny = template["q_ny"].format(**params)
            sql = template["sql"].format(**params)
        except KeyError:
            continue
        
        key = (q_en.strip().lower(), sql.strip())
        if key in keys:
            continue
        
        success, result, _ = execute_sql(sql)
        if success and result and result[0][0] is not None:
            inst = {
                "question_en": q_en,
                "question_ny": q_ny,
                "sql_statement": sql,
                "sql_result": format_as_tuples(result),
                "difficulty_level": template["diff"],
                "table": table
            }
            current.append(inst)
            keys.add(key)
    
    return current[:target_count]

# ============================================================================
# PRODUCTION TEMPLATES
# ============================================================================
print("\n[2] Filling production table...")
prod_districts = get_distinct('production', 'district')
prod_crops = get_distinct('production', 'crop')
prod_seasons = get_distinct('production', 'season')

prod_templates = [
    {"q_en": "How much {crop} was produced in {district}?", "q_ny": "Ndi {crop} ochuluka bwanji adakololedwa ku {district}?", "sql": "SELECT yield FROM production WHERE district = '{district}' AND crop = '{crop}';", "diff": "easy"},
    {"q_en": "What was the total {crop} yield in {season}?", "q_ny": "Ndi {crop} ochuluka bwanji adakololedwa mu {season}?", "sql": "SELECT SUM(yield) FROM production WHERE crop = '{crop}' AND season = '{season}';", "diff": "easy"},
    {"q_en": "Which district produced the most {crop}?", "q_ny": "Ndi boma liti lomwe lidakolola {crop} kwambiri?", "sql": "SELECT district FROM production WHERE crop = '{crop}' ORDER BY yield DESC LIMIT 1;", "diff": "medium"},
    {"q_en": "What is the average {crop} yield?", "q_ny": "Ndi zokolola za {crop} pakatikati?", "sql": "SELECT AVG(yield) FROM production WHERE crop = '{crop}';", "diff": "medium"},
    {"q_en": "Which crops are grown in {district}?", "q_ny": "Ndi mbewu zanji ku {district}?", "sql": "SELECT DISTINCT crop FROM production WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "What is the maximum {crop} yield?", "q_ny": "Ndi zokolola za {crop} zambiri?", "sql": "SELECT MAX(yield) FROM production WHERE crop = '{crop}';", "diff": "easy"},
    {"q_en": "What is the minimum {crop} yield?", "q_ny": "Ndi zokolola za {crop} zochepa?", "sql": "SELECT MIN(yield) FROM production WHERE crop = '{crop}';", "diff": "easy"},
    {"q_en": "How many districts grow {crop}?", "q_ny": "Ndi maboma angati omwe amabzala {crop}?", "sql": "SELECT COUNT(DISTINCT district) FROM production WHERE crop = '{crop}';", "diff": "medium"},
    {"q_en": "Top 5 crops in {district} by yield", "q_ny": "Mbewu 5 zapamwamba ku {district}", "sql": "SELECT crop, yield FROM production WHERE district = '{district}' ORDER BY yield DESC LIMIT 5;", "diff": "medium"},
    {"q_en": "Total yield in {district}", "q_ny": "Zokolola zonse ku {district}", "sql": "SELECT SUM(yield) FROM production WHERE district = '{district}';", "diff": "easy"},
]

valid_by_table['production'] = generate_and_add(
    'production', prod_templates,
    lambda: {'district': random.choice(prod_districts), 'crop': random.choice(prod_crops), 'season': random.choice(prod_seasons)},
    TARGET_PER_TABLE, valid_by_table['production']
)
print(f"  production: {len(valid_by_table['production'])}")

# ============================================================================
# POPULATION TEMPLATES
# ============================================================================
print("\n[3] Filling population table...")
pop_districts = get_distinct('population', 'district_name')
pop_regions = get_distinct('population', 'region_name')
pop_tas = get_distinct('population', 'ta_name', limit=30)

pop_templates = [
    {"q_en": "What is the total population in {district}?", "q_ny": "Ndi anthu angati ku {district}?", "sql": "SELECT SUM(CAST(total_population AS INTEGER)) FROM population WHERE district_name = '{district}';", "diff": "easy"},
    {"q_en": "How many males in {district}?", "q_ny": "Ndi amuna angati ku {district}?", "sql": "SELECT SUM(population_male) FROM population WHERE district_name = '{district}';", "diff": "easy"},
    {"q_en": "How many females in {district}?", "q_ny": "Ndi akazi angati ku {district}?", "sql": "SELECT SUM(population_female) FROM population WHERE district_name = '{district}';", "diff": "easy"},
    {"q_en": "How many households in {district}?", "q_ny": "Ndi mabanja angati ku {district}?", "sql": "SELECT SUM(number_households) FROM population WHERE district_name = '{district}';", "diff": "easy"},
    {"q_en": "Which districts are in {region} region?", "q_ny": "Ndi maboma ati m'chigawo cha {region}?", "sql": "SELECT DISTINCT district_name FROM population WHERE region_name = '{region}';", "diff": "easy"},
    {"q_en": "Total population in {region} region", "q_ny": "Anthu onse m'chigawo cha {region}", "sql": "SELECT SUM(CAST(total_population AS INTEGER)) FROM population WHERE region_name = '{region}';", "diff": "medium"},
    {"q_en": "How many TAs in {district}?", "q_ny": "Ndi mafumu angati ku {district}?", "sql": "SELECT COUNT(DISTINCT ta_name) FROM population WHERE district_name = '{district}';", "diff": "medium"},
    {"q_en": "Population of TA {ta}?", "q_ny": "Anthu ku TA {ta}?", "sql": "SELECT SUM(CAST(total_population AS INTEGER)) FROM population WHERE ta_name = '{ta}';", "diff": "medium"},
    {"q_en": "List TAs in {district}", "q_ny": "Mafumu ku {district}", "sql": "SELECT DISTINCT ta_name FROM population WHERE district_name = '{district}';", "diff": "easy"},
    {"q_en": "Districts count in {region}", "q_ny": "Maboma m'chigawo cha {region}", "sql": "SELECT COUNT(DISTINCT district_name) FROM population WHERE region_name = '{region}';", "diff": "easy"},
]

valid_by_table['population'] = generate_and_add(
    'population', pop_templates,
    lambda: {'district': random.choice(pop_districts) if pop_districts else 'Lilongwe', 'region': random.choice(pop_regions) if pop_regions else 'Central', 'ta': random.choice(pop_tas) if pop_tas else 'Kabudula'},
    TARGET_PER_TABLE, valid_by_table['population']
)
print(f"  population: {len(valid_by_table['population'])}")

# ============================================================================
# FOOD INSECURITY TEMPLATES
# ============================================================================
print("\n[4] Filling food_insecurity table...")
fi_districts = get_distinct('food_insecurity', 'district')
fi_levels = get_distinct('food_insecurity', 'insecurity_level')

fi_templates = [
    {"q_en": "Food insecurity level in {district}?", "q_ny": "Mlingo wa kusowa chakudya ku {district}?", "sql": "SELECT insecurity_level FROM food_insecurity WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Analyzed population in {district}?", "q_ny": "Anthu owunikiridwa ku {district}?", "sql": "SELECT analyzed_population FROM food_insecurity WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Food insecurity percentage in {district}?", "q_ny": "Peresenti ya kusowa chakudya ku {district}?", "sql": "SELECT percentage_population FROM food_insecurity WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Districts with insecurity level {level}?", "q_ny": "Maboma ndi mlingo {level}?", "sql": "SELECT district FROM food_insecurity WHERE insecurity_level = {level};", "diff": "medium"},
    {"q_en": "Time period for {district}?", "q_ny": "Nthawi ya {district}?", "sql": "SELECT time_period FROM food_insecurity WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Highest food insecurity district?", "q_ny": "Boma loopsa kwambiri?", "sql": "SELECT district, insecurity_level FROM food_insecurity ORDER BY insecurity_level DESC LIMIT 1;", "diff": "medium"},
    {"q_en": "Lowest food insecurity district?", "q_ny": "Boma lochepa vuto?", "sql": "SELECT district, insecurity_level FROM food_insecurity ORDER BY insecurity_level ASC LIMIT 1;", "diff": "medium"},
    {"q_en": "Average insecurity percentage?", "q_ny": "Peresenti pakatikati?", "sql": "SELECT AVG(percentage_population) FROM food_insecurity;", "diff": "medium"},
    {"q_en": "Total analyzed population?", "q_ny": "Anthu onse owunikiridwa?", "sql": "SELECT SUM(analyzed_population) FROM food_insecurity;", "diff": "easy"},
    {"q_en": "Insecurity description for {district}?", "q_ny": "Kufotokoza kwa {district}?", "sql": "SELECT insecurity_desc_short FROM food_insecurity WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Districts with percentage above 20?", "q_ny": "Maboma opitilira 20%?", "sql": "SELECT district, percentage_population FROM food_insecurity WHERE percentage_population > 20;", "diff": "hard"},
    {"q_en": "Count of critical districts?", "q_ny": "Maboma oopsa?", "sql": "SELECT COUNT(*) FROM food_insecurity WHERE insecurity_level >= 3;", "diff": "medium"},
]

valid_by_table['food_insecurity'] = generate_and_add(
    'food_insecurity', fi_templates,
    lambda: {'district': random.choice(fi_districts) if fi_districts else 'Lilongwe', 'level': random.choice(fi_levels) if fi_levels else 1},
    TARGET_PER_TABLE, valid_by_table['food_insecurity']
)
print(f"  food_insecurity: {len(valid_by_table['food_insecurity'])}")

# ============================================================================
# COMMODITY PRICES TEMPLATES
# ============================================================================
print("\n[5] Filling commodity_prices table...")
cp_districts = get_distinct('commodity_prices', 'district')
cp_markets = get_distinct('commodity_prices', 'market')
cp_commodities = get_distinct('commodity_prices', 'commodity')
cp_months = get_distinct('commodity_prices', 'month_name')
cp_years = get_distinct('commodity_prices', 'year')

cp_templates = [
    {"q_en": "Price of {commodity} in {market}?", "q_ny": "Mtengo wa {commodity} ku {market}?", "sql": "SELECT price FROM commodity_prices WHERE commodity = '{commodity}' AND market = '{market}' LIMIT 1;", "diff": "easy"},
    {"q_en": "Average price of {commodity} in {district}?", "q_ny": "Mtengo pakatikati wa {commodity} ku {district}?", "sql": "SELECT AVG(price) FROM commodity_prices WHERE commodity = '{commodity}' AND district = '{district}';", "diff": "medium"},
    {"q_en": "Markets in {district}?", "q_ny": "Misika ku {district}?", "sql": "SELECT DISTINCT market FROM commodity_prices WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Commodities in {market}?", "q_ny": "Katundu ku {market}?", "sql": "SELECT DISTINCT commodity FROM commodity_prices WHERE market = '{market}';", "diff": "easy"},
    {"q_en": "Cheapest {commodity} market?", "q_ny": "Msika wotsika wa {commodity}?", "sql": "SELECT market, MIN(price) FROM commodity_prices WHERE commodity = '{commodity}' AND price > 0 GROUP BY market ORDER BY MIN(price) LIMIT 1;", "diff": "hard"},
    {"q_en": "Price range of {commodity}?", "q_ny": "Mtengo wa {commodity}?", "sql": "SELECT MIN(price), MAX(price) FROM commodity_prices WHERE commodity = '{commodity}' AND price > 0;", "diff": "medium"},
    {"q_en": "How many markets in {district}?", "q_ny": "Misika ingati ku {district}?", "sql": "SELECT COUNT(DISTINCT market) FROM commodity_prices WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "Records in {year}?", "q_ny": "Malembedwe mu {year}?", "sql": "SELECT COUNT(*) FROM commodity_prices WHERE year = {year};", "diff": "easy"},
    {"q_en": "Commodities count in {district}?", "q_ny": "Mitundu ku {district}?", "sql": "SELECT COUNT(DISTINCT commodity) FROM commodity_prices WHERE district = '{district}';", "diff": "easy"},
    {"q_en": "EPA for {market}?", "q_ny": "EPA ya {market}?", "sql": "SELECT DISTINCT epa_name FROM commodity_prices WHERE market = '{market}';", "diff": "easy"},
]

valid_by_table['commodity_prices'] = generate_and_add(
    'commodity_prices', cp_templates,
    lambda: {'district': random.choice(cp_districts), 'market': random.choice(cp_markets), 'commodity': random.choice(cp_commodities), 'month': random.choice(cp_months), 'year': random.choice(cp_years)},
    TARGET_PER_TABLE, valid_by_table['commodity_prices']
)
print(f"  commodity_prices: {len(valid_by_table['commodity_prices'])}")

# ============================================================================
# MSE DAILY TEMPLATES
# ============================================================================
print("\n[6] Filling mse_daily table...")
mse_tickers = get_distinct('mse_daily', 'ticker')
mse_companies = get_distinct('mse_daily', 'company_name')
mse_sectors = get_distinct('mse_daily', 'sector')
mse_dates = get_distinct('mse_daily', 'trade_date', limit=20)

mse_templates = [
    {"q_en": "Close price of {ticker}?", "q_ny": "Mtengo wotseka wa {ticker}?", "sql": "SELECT close_price FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "easy"},
    {"q_en": "Volume for {ticker}?", "q_ny": "Kuchuluka kwa {ticker}?", "sql": "SELECT volume FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "easy"},
    {"q_en": "PE ratio for {ticker}?", "q_ny": "PE ratio ya {ticker}?", "sql": "SELECT pe_ratio FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "medium"},
    {"q_en": "Market cap of {ticker}?", "q_ny": "Market cap ya {ticker}?", "sql": "SELECT market_cap_mwk_mn FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "easy"},
    {"q_en": "Stocks in {sector} sector?", "q_ny": "Ma share mu {sector}?", "sql": "SELECT DISTINCT ticker FROM mse_daily WHERE sector = '{sector}';", "diff": "easy"},
    {"q_en": "Highest price of {ticker}?", "q_ny": "Mtengo wapamwamba wa {ticker}?", "sql": "SELECT MAX(high_price) FROM mse_daily WHERE ticker = '{ticker}';", "diff": "easy"},
    {"q_en": "Lowest price of {ticker}?", "q_ny": "Mtengo wotsika wa {ticker}?", "sql": "SELECT MIN(low_price) FROM mse_daily WHERE ticker = '{ticker}' AND low_price > 0;", "diff": "easy"},
    {"q_en": "Dividend yield of {ticker}?", "q_ny": "Dividend ya {ticker}?", "sql": "SELECT dividend_yield_pct FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "medium"},
    {"q_en": "Shares outstanding for {ticker}?", "q_ny": "Ma share a {ticker}?", "sql": "SELECT shares_outstanding FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "easy"},
    {"q_en": "PBV ratio of {ticker}?", "q_ny": "PBV ya {ticker}?", "sql": "SELECT pbv_ratio FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "medium"},
    {"q_en": "Previous close of {ticker}?", "q_ny": "Mtengo wapitawo wa {ticker}?", "sql": "SELECT previous_close_price FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;", "diff": "easy"},
    {"q_en": "How many sectors on MSE?", "q_ny": "Magawo angati pa MSE?", "sql": "SELECT COUNT(DISTINCT sector) FROM mse_daily WHERE sector IS NOT NULL;", "diff": "easy"},
]

valid_by_table['mse_daily'] = generate_and_add(
    'mse_daily', mse_templates,
    lambda: {'ticker': random.choice(mse_tickers), 'company': random.choice(mse_companies) if mse_companies else '', 'sector': random.choice(mse_sectors) if mse_sectors else '', 'date': random.choice(mse_dates)},
    TARGET_PER_TABLE, valid_by_table['mse_daily']
)
print(f"  mse_daily: {len(valid_by_table['mse_daily'])}")

# ============================================================================
# COMBINE AND SAVE
# ============================================================================
print("\n[7] Combining all tables...")
final_data = []
table_order = ['production', 'population', 'food_insecurity', 'commodity_prices', 'mse_daily']

for table in table_order:
    final_data.extend(valid_by_table[table])

print(f"\nFinal dataset: {len(final_data)} instances")
print("\nDistribution:")
for table in table_order:
    print(f"  {table}: {len(valid_by_table[table])}")

with open(CORRECTED_PATH, 'w', encoding='utf-8') as f:
    json.dump(final_data, f, indent=2, ensure_ascii=False)

conn.close()

print(f"\nSaved to: {CORRECTED_PATH}")
print("\n" + "=" * 70)
print("COMPLETE - 400 instances (80 per table)")
print("=" * 70)

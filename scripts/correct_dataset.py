#!/usr/bin/env python3
"""
Chichewa Text-to-SQL Dataset Correction Script
===============================================
Corrects SQL statements and regenerates instances to create a valid dataset.
"""

import json
import sqlite3
import re
import os
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
import random

random.seed(42)  # For reproducibility

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_PATH = os.path.join(BASE_DIR, "data", "database", "chichewa_text2sql.db")
DATASET_PATH = os.path.join(BASE_DIR, "data", "train", "train.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "train", "train_corrected.json")

TARGET_PER_TABLE = 80

# ============================================================================
# DATABASE HELPER
# ============================================================================

class DatabaseHelper:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
    def execute(self, sql: str) -> Tuple[bool, Any, Optional[str]]:
        """Execute SQL and return (success, result, error)."""
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            return True, results, None
        except sqlite3.Error as e:
            return False, None, str(e)
    
    def get_distinct_values(self, table: str, column: str, limit: int = 50) -> List[Any]:
        """Get distinct values from a column."""
        try:
            self.cursor.execute(f"SELECT DISTINCT {column} FROM {table} LIMIT {limit}")
            return [r[0] for r in self.cursor.fetchall() if r[0] is not None]
        except:
            return []
    
    def close(self):
        self.conn.close()


def format_result(result: Any) -> str:
    """Format SQL result for storage in JSON as proper SQL-style tuples."""
    if result is None:
        return "[]"
    
    if not result:
        return "[]"
    
    # Format all results as list of tuples (SQL result format)
    formatted = []
    for row in result:
        formatted_row = []
        for val in row:
            if isinstance(val, float):
                formatted_row.append(round(val, 2))
            else:
                formatted_row.append(val)
        # Convert to tuple representation
        formatted.append(tuple(formatted_row))
    
    # Return as string representation of list of tuples
    return str(formatted)


# ============================================================================
# SQL CORRECTION PATTERNS
# ============================================================================

# Column name corrections for population table
POPULATION_COLUMN_FIXES = {
    r'\bdistrict\b(?!\s*=)': 'district_name',
    r'(?<!district_)name(?!\s*2)': 'district_name',
    r'\bpopulation\b(?!_)': 'total_population',
    r'\bmale\b': 'population_male',
    r'\bfemale\b': 'population_female',
    r'\bhouseholds\b': 'number_households',
}

# Column corrections for food_insecurity
FOOD_INSECURITY_FIXES = {
    r'\bpopulation\b(?!_)': 'analyzed_population',
    r'\bperiod\b': 'time_period',
    r'\blevel\b': 'insecurity_level',
    r'\bpercentage\b': 'percentage_population',
}


def fix_sql_columns(sql: str, table: str) -> str:
    """Fix column references in SQL based on table."""
    fixed_sql = sql
    
    if table == 'population':
        # Fix common population column issues
        fixed_sql = re.sub(r"(?<![a-z_])district(?![_a-z])", "district_name", fixed_sql, flags=re.IGNORECASE)
        fixed_sql = re.sub(r"(?<![a-z_])population(?![_a-z])", "total_population", fixed_sql, flags=re.IGNORECASE)
        
    elif table == 'food_insecurity':
        # Fix common food_insecurity column issues  
        fixed_sql = re.sub(r"(?<![a-z_])population(?![_a-z])", "analyzed_population", fixed_sql, flags=re.IGNORECASE)
    
    # Fix table name issues
    fixed_sql = re.sub(r'\baverage_prices\b', 'commodity_prices', fixed_sql, flags=re.IGNORECASE)
    
    return fixed_sql


# ============================================================================
# INSTANCE TEMPLATES FOR EACH TABLE
# ============================================================================

def generate_production_instances(db: DatabaseHelper) -> List[Dict]:
    """Generate valid production table instances."""
    instances = []
    
    districts = db.get_distinct_values('production', 'district')
    crops = db.get_distinct_values('production', 'crop')
    seasons = db.get_distinct_values('production', 'season')
    
    templates = [
        # Easy queries
        {
            "template_en": "How much {crop} was produced in {district}?",
            "template_ny": "Ndi {crop} ochuluka bwanji adakololedwa ku {district}?",
            "sql_template": "SELECT yield FROM production WHERE district = '{district}' AND crop = '{crop}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What was the total {crop} yield in {season}?",
            "template_ny": "Ndi {crop} ochuluka bwanji adakololedwa mu {season}?",
            "sql_template": "SELECT SUM(yield) FROM production WHERE crop = '{crop}' AND season = '{season}';",
            "difficulty": "easy"
        },
        {
            "template_en": "Which crops were grown in {district}?",
            "template_ny": "Ndi mbewu zanji zomwe zidabzalidwa ku {district}?",
            "sql_template": "SELECT DISTINCT crop FROM production WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the yield of {crop} in {district} during {season}?",
            "template_ny": "Ndi zokolola za {crop} zochuluka bwanji ku {district} mu {season}?",
            "sql_template": "SELECT yield FROM production WHERE district = '{district}' AND crop = '{crop}' AND season = '{season}';",
            "difficulty": "easy"
        },
        # Medium queries
        {
            "template_en": "Which district produced the most {crop}?",
            "template_ny": "Ndi boma liti lomwe lidakolola {crop} kwambiri?",
            "sql_template": "SELECT district FROM production WHERE crop = '{crop}' ORDER BY yield DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the average {crop} yield across all districts?",
            "template_ny": "Ndi zokolola za {crop} zomwe zimapezeka pakatikati m'maboma onse?",
            "sql_template": "SELECT AVG(yield) FROM production WHERE crop = '{crop}';",
            "difficulty": "medium"
        },
        {
            "template_en": "Which crops performed well in {district}?",
            "template_ny": "Ndi mbewu zanji zomwe zidachita bwino ku {district}?",
            "sql_template": "SELECT crop, yield FROM production WHERE district = '{district}' ORDER BY yield DESC LIMIT 5;",
            "difficulty": "medium"
        },
        {
            "template_en": "How many different crops are grown in {district}?",
            "template_ny": "Ndi mitundu ingati ya mbewu yomwe imabzalidwa ku {district}?",
            "sql_template": "SELECT COUNT(DISTINCT crop) FROM production WHERE district = '{district}';",
            "difficulty": "medium"
        },
        # Hard queries
        {
            "template_en": "Which districts have {crop} yield above average?",
            "template_ny": "Ndi maboma ati omwe ali ndi zokolola za {crop} kupitilira pakatikati?",
            "sql_template": "SELECT district, yield FROM production WHERE crop = '{crop}' AND yield > (SELECT AVG(yield) FROM production WHERE crop = '{crop}');",
            "difficulty": "hard"
        },
        {
            "template_en": "What is the total production of all crops in {district}?",
            "template_ny": "Ndi zokolola zonse zochuluka bwanji ku {district}?",
            "sql_template": "SELECT SUM(yield) FROM production WHERE district = '{district}';",
            "difficulty": "medium"
        },
        {
            "template_en": "Compare {crop} production between {district} and other districts",
            "template_ny": "Yerekezani zokolola za {crop} pakati pa {district} ndi maboma ena",
            "sql_template": "SELECT district, yield FROM production WHERE crop = '{crop}' ORDER BY yield DESC LIMIT 5;",
            "difficulty": "hard"
        },
        {
            "template_en": "What was the minimum {crop} yield recorded?",
            "template_ny": "Ndi zokolola za {crop} zochepa kwambiri zomwe zidalembedwa?",
            "sql_template": "SELECT MIN(yield) FROM production WHERE crop = '{crop}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What was the maximum {crop} yield recorded?",
            "template_ny": "Ndi zokolola za {crop} zambiri kwambiri zomwe zidalembedwa?",
            "sql_template": "SELECT MAX(yield) FROM production WHERE crop = '{crop}';",
            "difficulty": "easy"
        },
        {
            "template_en": "List all districts that grow {crop}",
            "template_ny": "Lembani maboma onse omwe amabzala {crop}",
            "sql_template": "SELECT DISTINCT district FROM production WHERE crop = '{crop}';",
            "difficulty": "easy"
        },
        {
            "template_en": "Top 3 crops by yield in {district}",
            "template_ny": "Mbewu zitatu zapamwamba mwa zokolola ku {district}",
            "sql_template": "SELECT crop, yield FROM production WHERE district = '{district}' ORDER BY yield DESC LIMIT 3;",
            "difficulty": "medium"
        },
    ]
    
    count = 0
    template_idx = 0
    
    while count < TARGET_PER_TABLE:
        template = templates[template_idx % len(templates)]
        district = random.choice(districts)
        crop = random.choice(crops)
        season = random.choice(seasons) if seasons else '2023-2024'
        
        question_en = template["template_en"].format(crop=crop, district=district, season=season)
        question_ny = template["template_ny"].format(crop=crop, district=district, season=season)
        sql = template["sql_template"].format(crop=crop, district=district, season=season)
        
        # Execute and check result
        success, result, error = db.execute(sql)
        if success and result:
            instances.append({
                "question_en": question_en,
                "question_ny": question_ny,
                "sql_statement": sql,
                "sql_result": format_result(result),
                "difficulty_level": template["difficulty"],
                "table": "production"
            })
            count += 1
        
        template_idx += 1
        if template_idx > len(templates) * len(districts):
            break
    
    return instances[:TARGET_PER_TABLE]


def generate_population_instances(db: DatabaseHelper) -> List[Dict]:
    """Generate valid population table instances."""
    instances = []
    
    districts = db.get_distinct_values('population', 'district_name')
    regions = db.get_distinct_values('population', 'region_name')
    tas = db.get_distinct_values('population', 'ta_name', limit=30)
    
    templates = [
        {
            "template_en": "What is the total population in {district}?",
            "template_ny": "Ndi anthu angati onse okhala ku {district}?",
            "sql_template": "SELECT SUM(CAST(total_population AS INTEGER)) FROM population WHERE district_name = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "How many males live in {district}?",
            "template_ny": "Ndi amuna angati okhala ku {district}?",
            "sql_template": "SELECT SUM(population_male) FROM population WHERE district_name = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "How many females live in {district}?",
            "template_ny": "Ndi akazi angati okhala ku {district}?",
            "sql_template": "SELECT SUM(population_female) FROM population WHERE district_name = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "How many households are in {district}?",
            "template_ny": "Ndi mabanja angati ku {district}?",
            "sql_template": "SELECT SUM(number_households) FROM population WHERE district_name = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the population of {ta_name} traditional authority?",
            "template_ny": "Ndi anthu angati ku TA {ta_name}?",
            "sql_template": "SELECT SUM(CAST(total_population AS INTEGER)) FROM population WHERE ta_name = '{ta_name}';",
            "difficulty": "medium"
        },
        {
            "template_en": "Which districts are in the {region} region?",
            "template_ny": "Ndi maboma ati omwe ali m'chigawo cha {region}?",
            "sql_template": "SELECT DISTINCT district_name FROM population WHERE region_name = '{region}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the total population in {region} region?",
            "template_ny": "Ndi anthu angati onse m'chigawo cha {region}?",
            "sql_template": "SELECT SUM(CAST(total_population AS INTEGER)) FROM population WHERE region_name = '{region}';",
            "difficulty": "medium"
        },
        {
            "template_en": "How many traditional authorities are in {district}?",
            "template_ny": "Ndi mafumu aakulu angati ku {district}?",
            "sql_template": "SELECT COUNT(DISTINCT ta_name) FROM population WHERE district_name = '{district}';",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the male to female ratio in {district}?",
            "template_ny": "Ndi kuchuluka kwa amuna ndi akazi ku {district}?",
            "sql_template": "SELECT SUM(population_male), SUM(population_female) FROM population WHERE district_name = '{district}';",
            "difficulty": "medium"
        },
        {
            "template_en": "Which district has the most households?",
            "template_ny": "Ndi boma liti lomwe lili ndi mabanja ambiri?",
            "sql_template": "SELECT district_name, SUM(number_households) as total FROM population GROUP BY district_name ORDER BY total DESC LIMIT 1;",
            "difficulty": "hard"
        },
        {
            "template_en": "What is the average household size in {district}?",
            "template_ny": "Ndi kukula kwa banja pakatikati ku {district}?",
            "sql_template": "SELECT ROUND(SUM(CAST(total_population AS REAL)) / SUM(number_households), 2) FROM population WHERE district_name = '{district}';",
            "difficulty": "hard"
        },
        {
            "template_en": "List all traditional authorities in {district}",
            "template_ny": "Lembani mafumu onse ku {district}",
            "sql_template": "SELECT DISTINCT ta_name FROM population WHERE district_name = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "Which region has the largest population?",
            "template_ny": "Ndi chigawo chiti chomwe chili ndi anthu ambiri?",
            "sql_template": "SELECT region_name, SUM(CAST(total_population AS INTEGER)) as total FROM population GROUP BY region_name ORDER BY total DESC LIMIT 1;",
            "difficulty": "hard"
        },
        {
            "template_en": "How many enumeration areas are in {district}?",
            "template_ny": "Ndi malo angati owerengera anthu ku {district}?",
            "sql_template": "SELECT COUNT(DISTINCT ea_code) FROM population WHERE district_name = '{district}';",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the total number of districts in {region} region?",
            "template_ny": "Ndi maboma angati m'chigawo cha {region}?",
            "sql_template": "SELECT COUNT(DISTINCT district_name) FROM population WHERE region_name = '{region}';",
            "difficulty": "easy"
        },
    ]
    
    count = 0
    template_idx = 0
    used_combinations = set()
    
    while count < TARGET_PER_TABLE and template_idx < len(templates) * 100:
        template = templates[template_idx % len(templates)]
        district = random.choice(districts) if districts else 'Lilongwe'
        region = random.choice(regions) if regions else 'Central'
        ta_name = random.choice(tas) if tas else 'Kabudula'
        
        combo_key = f"{template_idx % len(templates)}_{district}_{region}_{ta_name}"
        if combo_key in used_combinations:
            template_idx += 1
            continue
        used_combinations.add(combo_key)
        
        question_en = template["template_en"].format(district=district, region=region, ta_name=ta_name)
        question_ny = template["template_ny"].format(district=district, region=region, ta_name=ta_name)
        sql = template["sql_template"].format(district=district, region=region, ta_name=ta_name)
        
        success, result, error = db.execute(sql)
        if success and result and result[0][0] is not None:
            instances.append({
                "question_en": question_en,
                "question_ny": question_ny,
                "sql_statement": sql,
                "sql_result": format_result(result),
                "difficulty_level": template["difficulty"],
                "table": "population"
            })
            count += 1
        
        template_idx += 1
    
    return instances[:TARGET_PER_TABLE]


def generate_food_insecurity_instances(db: DatabaseHelper) -> List[Dict]:
    """Generate valid food_insecurity table instances."""
    instances = []
    
    districts = db.get_distinct_values('food_insecurity', 'district')
    periods = db.get_distinct_values('food_insecurity', 'time_period')
    levels = db.get_distinct_values('food_insecurity', 'insecurity_level')
    
    templates = [
        {
            "template_en": "What is the food insecurity level in {district}?",
            "template_ny": "Ndi mlingo wanji wa kusowa chakudya ku {district}?",
            "sql_template": "SELECT insecurity_level, insecurity_desc_short FROM food_insecurity WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "How many people are analyzed for food insecurity in {district}?",
            "template_ny": "Ndi anthu angati omwe akuwunikiridwa za kusowa chakudya ku {district}?",
            "sql_template": "SELECT analyzed_population FROM food_insecurity WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What percentage of population faces food insecurity in {district}?",
            "template_ny": "Ndi peresenti yanji ya anthu omwe akukumana ndi vuto la kusowa chakudya ku {district}?",
            "sql_template": "SELECT percentage_population FROM food_insecurity WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "Which districts have food insecurity level {level}?",
            "template_ny": "Ndi maboma ati omwe ali ndi mlingo wa kusowa chakudya {level}?",
            "sql_template": "SELECT district FROM food_insecurity WHERE insecurity_level = {level};",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the time period for food insecurity analysis in {district}?",
            "template_ny": "Ndi nthawi yanji yomwe anawunikiridwa za kusowa chakudya ku {district}?",
            "sql_template": "SELECT time_period FROM food_insecurity WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "Describe the food insecurity situation in {district}",
            "template_ny": "Fotokozani za vuto la kusowa chakudya ku {district}",
            "sql_template": "SELECT insecurity_desc_long FROM food_insecurity WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "Which district has the highest food insecurity level?",
            "template_ny": "Ndi boma liti lomwe lili ndi vuto lalikulu la kusowa chakudya?",
            "sql_template": "SELECT district, insecurity_level FROM food_insecurity ORDER BY insecurity_level DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "Which district has the lowest food insecurity?",
            "template_ny": "Ndi boma liti lomwe lili ndi vuto lochepa la kusowa chakudya?",
            "sql_template": "SELECT district, insecurity_level FROM food_insecurity ORDER BY insecurity_level ASC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "How many districts have critical food insecurity?",
            "template_ny": "Ndi maboma angati omwe ali ndi vuto loopsa la kusowa chakudya?",
            "sql_template": "SELECT COUNT(*) FROM food_insecurity WHERE insecurity_level >= 3;",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the average food insecurity percentage across all districts?",
            "template_ny": "Ndi peresenti yapakatikati ya kusowa chakudya m'maboma onse?",
            "sql_template": "SELECT AVG(percentage_population) FROM food_insecurity;",
            "difficulty": "medium"
        },
        {
            "template_en": "List all districts with their food insecurity levels",
            "template_ny": "Lembani maboma onse ndi milingo yawo ya kusowa chakudya",
            "sql_template": "SELECT district, insecurity_level FROM food_insecurity ORDER BY insecurity_level DESC;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the total analyzed population for food insecurity?",
            "template_ny": "Ndi anthu angati onse omwe anawunikiridwa za kusowa chakudya?",
            "sql_template": "SELECT SUM(analyzed_population) FROM food_insecurity;",
            "difficulty": "easy"
        },
        {
            "template_en": "Which districts have food insecurity percentage above 20?",
            "template_ny": "Ndi maboma ati omwe ali ndi peresenti ya kusowa chakudya kupitilira 20?",
            "sql_template": "SELECT district, percentage_population FROM food_insecurity WHERE percentage_population > 20;",
            "difficulty": "hard"
        },
        {
            "template_en": "Compare food insecurity between {district} and other districts",
            "template_ny": "Yerekezani kusowa chakudya pakati pa {district} ndi maboma ena",
            "sql_template": "SELECT district, insecurity_level, percentage_population FROM food_insecurity ORDER BY insecurity_level DESC LIMIT 5;",
            "difficulty": "hard"
        },
        {
            "template_en": "What is the maximum analyzed population in any district?",
            "template_ny": "Ndi anthu ochuluka kwambiri omwe anawunikiridwa ku boma lililonse?",
            "sql_template": "SELECT MAX(analyzed_population) FROM food_insecurity;",
            "difficulty": "easy"
        },
    ]
    
    count = 0
    template_idx = 0
    used_combinations = set()
    
    while count < TARGET_PER_TABLE and template_idx < len(templates) * 100:
        template = templates[template_idx % len(templates)]
        district = random.choice(districts) if districts else 'Lilongwe'
        level = random.choice(levels) if levels else 1
        
        combo_key = f"{template_idx % len(templates)}_{district}_{level}"
        if combo_key in used_combinations:
            template_idx += 1
            continue
        used_combinations.add(combo_key)
        
        question_en = template["template_en"].format(district=district, level=level)
        question_ny = template["template_ny"].format(district=district, level=level)
        sql = template["sql_template"].format(district=district, level=level)
        
        success, result, error = db.execute(sql)
        if success and result and (len(result) > 0):
            instances.append({
                "question_en": question_en,
                "question_ny": question_ny,
                "sql_statement": sql,
                "sql_result": format_result(result),
                "difficulty_level": template["difficulty"],
                "table": "food_insecurity"
            })
            count += 1
        
        template_idx += 1
    
    return instances[:TARGET_PER_TABLE]


def generate_commodity_prices_instances(db: DatabaseHelper) -> List[Dict]:
    """Generate valid commodity_prices table instances."""
    instances = []
    
    districts = db.get_distinct_values('commodity_prices', 'district')
    markets = db.get_distinct_values('commodity_prices', 'market')
    commodities = db.get_distinct_values('commodity_prices', 'commodity')
    months = db.get_distinct_values('commodity_prices', 'month_name')
    years = db.get_distinct_values('commodity_prices', 'year')
    
    templates = [
        {
            "template_en": "What is the price of {commodity} in {market} market?",
            "template_ny": "Ndi mtengo wanji wa {commodity} ku msika wa {market}?",
            "sql_template": "SELECT price FROM commodity_prices WHERE commodity = '{commodity}' AND market = '{market}' LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the average price of {commodity} in {district}?",
            "template_ny": "Ndi mtengo wapakatikati wa {commodity} ku {district}?",
            "sql_template": "SELECT AVG(price) FROM commodity_prices WHERE commodity = '{commodity}' AND district = '{district}';",
            "difficulty": "medium"
        },
        {
            "template_en": "Which market has the cheapest {commodity}?",
            "template_ny": "Ndi msika uti womwe uli ndi {commodity} wotsika mtengo?",
            "sql_template": "SELECT market, MIN(price) FROM commodity_prices WHERE commodity = '{commodity}' AND price > 0 GROUP BY market ORDER BY MIN(price) ASC LIMIT 1;",
            "difficulty": "hard"
        },
        {
            "template_en": "Which market has the most expensive {commodity}?",
            "template_ny": "Ndi msika uti womwe uli ndi {commodity} wodula kwambiri?",
            "sql_template": "SELECT market, MAX(price) FROM commodity_prices WHERE commodity = '{commodity}' GROUP BY market ORDER BY MAX(price) DESC LIMIT 1;",
            "difficulty": "hard"
        },
        {
            "template_en": "What commodities are sold in {market} market?",
            "template_ny": "Ndi katundu wanji womwe amagulitsidwa ku msika wa {market}?",
            "sql_template": "SELECT DISTINCT commodity FROM commodity_prices WHERE market = '{market}';",
            "difficulty": "easy"
        },
        {
            "template_en": "How many markets are in {district}?",
            "template_ny": "Ndi misika ingati ku {district}?",
            "sql_template": "SELECT COUNT(DISTINCT market) FROM commodity_prices WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What was the price of {commodity} in {month}?",
            "template_ny": "Mtengo wa {commodity} unali bwanji mu {month}?",
            "sql_template": "SELECT AVG(price) FROM commodity_prices WHERE commodity = '{commodity}' AND month_name = '{month}';",
            "difficulty": "medium"
        },
        {
            "template_en": "List all markets in {district}",
            "template_ny": "Lembani misika yonse ku {district}",
            "sql_template": "SELECT DISTINCT market FROM commodity_prices WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the price range of {commodity}?",
            "template_ny": "Ndi mtengo wochepa ndi wokwera wa {commodity}?",
            "sql_template": "SELECT MIN(price), MAX(price) FROM commodity_prices WHERE commodity = '{commodity}' AND price > 0;",
            "difficulty": "medium"
        },
        {
            "template_en": "Which district has the lowest average price for {commodity}?",
            "template_ny": "Ndi boma liti lomwe lili ndi mtengo wotsika wa {commodity}?",
            "sql_template": "SELECT district, AVG(price) as avg_price FROM commodity_prices WHERE commodity = '{commodity}' AND price > 0 GROUP BY district ORDER BY avg_price ASC LIMIT 1;",
            "difficulty": "hard"
        },
        {
            "template_en": "How many different commodities are tracked in {district}?",
            "template_ny": "Ndi mitundu ingati ya katundu yomwe imalembedwa ku {district}?",
            "sql_template": "SELECT COUNT(DISTINCT commodity) FROM commodity_prices WHERE district = '{district}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the total number of price records in {year}?",
            "template_ny": "Ndi malembedwe angati a mitengo mu {year}?",
            "sql_template": "SELECT COUNT(*) FROM commodity_prices WHERE year = {year};",
            "difficulty": "easy"
        },
        {
            "template_en": "List the top 5 most expensive commodities in {district}",
            "template_ny": "Lembani katundu 5 wodula kwambiri ku {district}",
            "sql_template": "SELECT commodity, MAX(price) as max_price FROM commodity_prices WHERE district = '{district}' GROUP BY commodity ORDER BY max_price DESC LIMIT 5;",
            "difficulty": "hard"
        },
        {
            "template_en": "What is the collection date for prices in {market}?",
            "template_ny": "Ndi tsiku lanji lomwe anasonkhanitsa mitengo ku {market}?",
            "sql_template": "SELECT DISTINCT collection_date FROM commodity_prices WHERE market = '{market}' LIMIT 5;",
            "difficulty": "easy"
        },
        {
            "template_en": "Which EPA covers {market} market?",
            "template_ny": "Ndi EPA yanji yomwe imayangana msika wa {market}?",
            "sql_template": "SELECT DISTINCT epa_name FROM commodity_prices WHERE market = '{market}';",
            "difficulty": "easy"
        },
    ]
    
    count = 0
    template_idx = 0
    used_combinations = set()
    
    while count < TARGET_PER_TABLE and template_idx < len(templates) * 100:
        template = templates[template_idx % len(templates)]
        district = random.choice(districts) if districts else 'Lilongwe'
        market = random.choice(markets) if markets else 'Lilongwe'
        commodity = random.choice(commodities) if commodities else 'Maize'
        month = random.choice(months) if months else 'January'
        year = random.choice(years) if years else 2024
        
        combo_key = f"{template_idx % len(templates)}_{district}_{market}_{commodity}_{month}_{year}"
        if combo_key in used_combinations:
            template_idx += 1
            continue
        used_combinations.add(combo_key)
        
        question_en = template["template_en"].format(
            district=district, market=market, commodity=commodity, month=month, year=year
        )
        question_ny = template["template_ny"].format(
            district=district, market=market, commodity=commodity, month=month, year=year
        )
        sql = template["sql_template"].format(
            district=district, market=market, commodity=commodity, month=month, year=year
        )
        
        success, result, error = db.execute(sql)
        if success and result and (len(result) > 0) and result[0][0] is not None:
            instances.append({
                "question_en": question_en,
                "question_ny": question_ny,
                "sql_statement": sql,
                "sql_result": format_result(result),
                "difficulty_level": template["difficulty"],
                "table": "commodity_prices"
            })
            count += 1
        
        template_idx += 1
    
    return instances[:TARGET_PER_TABLE]


def generate_mse_daily_instances(db: DatabaseHelper) -> List[Dict]:
    """Generate valid mse_daily (stock exchange) table instances."""
    instances = []
    
    tickers = db.get_distinct_values('mse_daily', 'ticker')
    companies = db.get_distinct_values('mse_daily', 'company_name')
    sectors = db.get_distinct_values('mse_daily', 'sector')
    dates = db.get_distinct_values('mse_daily', 'trade_date', limit=20)
    
    templates = [
        {
            "template_en": "What is the close price of {ticker} stock?",
            "template_ny": "Ndi mtengo wotsekedwa wa share ya {ticker}?",
            "sql_template": "SELECT close_price FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the trading volume for {ticker}?",
            "template_ny": "Ndi kuchuluka kwa malonda a share ya {ticker}?",
            "sql_template": "SELECT volume FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the PE ratio for {company}?",
            "template_ny": "Ndi PE ratio ya {company}?",
            "sql_template": "SELECT pe_ratio FROM mse_daily WHERE company_name = '{company}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the market cap of {ticker}?",
            "template_ny": "Ndi market cap ya {ticker}?",
            "sql_template": "SELECT market_cap_mwk_mn FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "Which stocks are in the {sector} sector?",
            "template_ny": "Ndi ma share ati omwe ali mu gawo la {sector}?",
            "sql_template": "SELECT DISTINCT ticker, company_name FROM mse_daily WHERE sector = '{sector}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What was the highest price of {ticker}?",
            "template_ny": "Mtengo wokwera kwambiri wa {ticker} unali bwanji?",
            "sql_template": "SELECT MAX(high_price) FROM mse_daily WHERE ticker = '{ticker}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What was the lowest price of {ticker}?",
            "template_ny": "Mtengo wotsika kwambiri wa {ticker} unali bwanji?",
            "sql_template": "SELECT MIN(low_price) FROM mse_daily WHERE ticker = '{ticker}' AND low_price > 0;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the dividend yield of {ticker}?",
            "template_ny": "Ndi dividend yield ya {ticker}?",
            "sql_template": "SELECT dividend_yield_pct FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "What is the earnings yield of {ticker}?",
            "template_ny": "Ndi earnings yield ya {ticker}?",
            "sql_template": "SELECT earnings_yield_pct FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "How many shares are outstanding for {ticker}?",
            "template_ny": "Ndi ma share angati a {ticker} omwe ali panja?",
            "sql_template": "SELECT shares_outstanding FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the profit after tax for {company}?",
            "template_ny": "Ndi phindu pambuyo pa msonkho la {company}?",
            "sql_template": "SELECT profit_after_tax_mwk_mn FROM mse_daily WHERE company_name = '{company}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "List all stocks traded on {date}",
            "template_ny": "Lembani ma share onse omwe anagulitsidwa pa {date}",
            "sql_template": "SELECT DISTINCT ticker, company_name FROM mse_daily WHERE trade_date = '{date}';",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the bid price for {ticker}?",
            "template_ny": "Ndi mtengo wogula wa {ticker}?",
            "sql_template": "SELECT bid_price FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "What is the ask price for {ticker}?",
            "template_ny": "Ndi mtengo wogulitsa wa {ticker}?",
            "sql_template": "SELECT ask_price FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
        {
            "template_en": "Which stock has the highest market cap?",
            "template_ny": "Ndi share yiti yomwe ili ndi market cap yayikulu?",
            "sql_template": "SELECT ticker, company_name, MAX(market_cap_mwk_mn) FROM mse_daily WHERE market_cap_mwk_mn IS NOT NULL;",
            "difficulty": "hard"
        },
        {
            "template_en": "What is the PBV ratio for {ticker}?",
            "template_ny": "Ndi PBV ratio ya {ticker}?",
            "sql_template": "SELECT pbv_ratio FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "medium"
        },
        {
            "template_en": "How many different sectors are listed on MSE?",
            "template_ny": "Ndi magawo angati osiyanasiyana omwe alipo pa MSE?",
            "sql_template": "SELECT COUNT(DISTINCT sector) FROM mse_daily WHERE sector IS NOT NULL;",
            "difficulty": "easy"
        },
        {
            "template_en": "What was the previous close price of {ticker}?",
            "template_ny": "Mtengo womaliza wapitawo wa {ticker} unali bwanji?",
            "sql_template": "SELECT previous_close_price FROM mse_daily WHERE ticker = '{ticker}' ORDER BY trade_date DESC LIMIT 1;",
            "difficulty": "easy"
        },
    ]
    
    count = 0
    template_idx = 0
    used_combinations = set()
    
    while count < TARGET_PER_TABLE and template_idx < len(templates) * 100:
        template = templates[template_idx % len(templates)]
        ticker = random.choice(tickers) if tickers else 'AIRTEL'
        company = random.choice(companies) if companies else 'Airtel Malawi'
        sector = random.choice(sectors) if sectors else 'Banking'
        date = random.choice(dates) if dates else '2025-02-06'
        
        combo_key = f"{template_idx % len(templates)}_{ticker}_{company}_{sector}_{date}"
        if combo_key in used_combinations:
            template_idx += 1
            continue
        used_combinations.add(combo_key)
        
        question_en = template["template_en"].format(
            ticker=ticker, company=company, sector=sector, date=date
        )
        question_ny = template["template_ny"].format(
            ticker=ticker, company=company, sector=sector, date=date
        )
        sql = template["sql_template"].format(
            ticker=ticker, company=company, sector=sector, date=date
        )
        
        success, result, error = db.execute(sql)
        if success and result and (len(result) > 0) and result[0][0] is not None:
            instances.append({
                "question_en": question_en,
                "question_ny": question_ny,
                "sql_statement": sql,
                "sql_result": format_result(result),
                "difficulty_level": template["difficulty"],
                "table": "mse_daily"
            })
            count += 1
        
        template_idx += 1
    
    return instances[:TARGET_PER_TABLE]


def main():
    print("=" * 70)
    print("CHICHEWA TEXT-TO-SQL DATASET CORRECTION")
    print("=" * 70 + "\n")
    
    # Connect to database
    print("[1] Connecting to database...")
    db = DatabaseHelper(DATABASE_PATH)
    
    # Generate instances for each table
    all_instances = []
    
    print("[2] Generating production instances...")
    production_instances = generate_production_instances(db)
    print(f"    Generated {len(production_instances)} production instances")
    all_instances.extend(production_instances)
    
    print("[3] Generating population instances...")
    population_instances = generate_population_instances(db)
    print(f"    Generated {len(population_instances)} population instances")
    all_instances.extend(population_instances)
    
    print("[4] Generating food_insecurity instances...")
    food_insecurity_instances = generate_food_insecurity_instances(db)
    print(f"    Generated {len(food_insecurity_instances)} food_insecurity instances")
    all_instances.extend(food_insecurity_instances)
    
    print("[5] Generating commodity_prices instances...")
    commodity_prices_instances = generate_commodity_prices_instances(db)
    print(f"    Generated {len(commodity_prices_instances)} commodity_prices instances")
    all_instances.extend(commodity_prices_instances)
    
    print("[6] Generating mse_daily instances...")
    mse_daily_instances = generate_mse_daily_instances(db)
    print(f"    Generated {len(mse_daily_instances)} mse_daily instances")
    all_instances.extend(mse_daily_instances)
    
    db.close()
    
    # Save corrected dataset
    print(f"\n[7] Saving corrected dataset to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_instances, f, indent=2, ensure_ascii=False)
    
    # Summary
    print("\n" + "=" * 70)
    print("CORRECTION COMPLETE")
    print("=" * 70)
    print(f"Total instances: {len(all_instances)}")
    
    # Count by table
    table_counts = {}
    difficulty_counts = {}
    for inst in all_instances:
        table = inst['table']
        diff = inst['difficulty_level']
        table_counts[table] = table_counts.get(table, 0) + 1
        difficulty_counts[diff] = difficulty_counts.get(diff, 0) + 1
    
    print("\nBy table:")
    for table, count in sorted(table_counts.items()):
        print(f"  {table}: {count}")
    
    print("\nBy difficulty:")
    for diff, count in sorted(difficulty_counts.items()):
        print(f"  {diff}: {count}")
    
    print(f"\nOutput saved to: {OUTPUT_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    main()

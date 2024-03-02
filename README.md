# Simple-Meal Recipe Data Engineering

This repository contains a collection of Python scripts that are used to scrape, clean, and process food recipe data. The data is then used to generate a SQL seed file for initializing a database.

## Project Structure

The project is structured as follows:

- [`01-recipe_tags.py`](01-recipe_tags.py): This script is used to extract tags from the raw recipe data.
- [`02-clean_tags.py`](02-clean_tags.py): This script is used to clean the extracted tags.
- [`03-recipe_cleaning.py`](03-recipe_cleaning.py): This script is used to clean the raw recipe data.
- [`04-scrape_ingredients.py`](04-scrape_ingredients.py): This script is used to scrape ingredient data from the web.
- [`05-match-ingredient-prices.py`](05-match-ingredient-prices.py): This script is used to match ingredient prices to the scraped ingredient data.
- [`99-generate_sql_seed.py`](99-generate_sql_seed.py): This script is used to generate a SQL seed file from the processed data.

The [`food-com-recipes/`](food-com-recipes/) directory contains the raw recipe data.

The [`data/`](data/) directory contains various CSV and YAML files that are used as inputs and outputs by the scripts.

The [`experiments/`](experiments/) directory contains experimental scripts that were used during the initial dataset exploration.

## How to Use

1. Run the scripts in the order of their numbering.
2. The final output will be a SQL seed file ([`data/seed.sql`](data/seed.sql)) that can be used to initialize a database.

## Dependencies

The scripts in this repository depend on several Python libraries, including Polars, BeautifulSoup, and Pint. The required libraries can be installed using the provided [`requirements.txt`](requirements.txt)
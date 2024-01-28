import ast
import csv
import re
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import yaml

# File paths
CSV_FILES = [
    Path("food-com-recipes/PP_recipes.csv"),
    Path("food-com-recipes/RAW_recipes.csv"),
]
PKL_FILE = Path("food-com-recipes/ingr_map.pkl")
CLEANED_RECIPES_CSV = Path("food-com-recipes/cleaned_recipes.csv")
TAGS_FILE = Path("food-com-recipes/tags.yaml")

# Set pd seed
np.random.seed(42)

# wide display
st.set_page_config(layout="wide")


# Load the csv files and save the top 50 rows into a new CSV file
def save_first_50_lines_of_csv():
    for csv_file in CSV_FILES:
        with csv_file.open("r") as f:
            reader = csv.reader(f)
            with Path(csv_file.stem + "_first50.csv").open("w") as f1:
                writer = csv.writer(f1)
                for i in range(50):
                    writer.writerow(next(reader))


with st.spinner("Importing data..."):
    # save_first_50_lines_of_csv()
    # raw_recipes_full = pd.read_csv(CSV_FILES[1])
    # raw_recipes = raw_recipes_full.head(10000).copy()
    raw_recipes = pd.read_csv(CSV_FILES[1])
    # pp_recipes = pd.read_csv(CSV_FILES[0])
st.dataframe(raw_recipes.head(20))


# ==============================================================================
def split_nutrition(df):
    """Split the 'nutrition' column into individual components."""

    # Define individual components
    nutririon_categories = [
        "calories",
        "total_fat_pdv",
        "sugar_pdv",
        "sodium_pdv",
        "protein_pdv",
        "saturated_fat_pdv",
        "carbohydrate_pdv",
    ]

    # Split the nutrition column into individual columns
    df[nutririon_categories] = pd.DataFrame(map(ast.literal_eval, df["nutrition"]))

    # Drop the original 'nutrition' column
    df = df.drop("nutrition", axis=1)

    return df


with st.spinner("Splitting nutrition..."):
    recipes = split_nutrition(raw_recipes)
st.dataframe(recipes.head(20))

# df = raw_recipes["tags"]
# df.to_csv("tags.csv")

# ==============================================================================


def load_tags(tags_file: Path) -> list:
    """Load the tags from the yaml file."""
    tags = yaml.safe_load(tags_file.open("r"))

    def compile_regex(patterns: list) -> re.Pattern:
        """Compile a list of regex patterns into a single pattern."""

        combined_pattern = f"""
            '                               # Match opening quote
            (                               # Start capture group 1
                [^']*?                      # Match anything but a quote
                (?:                         # Start non-capturing group
                    {'|'.join(patterns)}    # Match any of the patterns
                )                           # End non-capturing group
                [^']*?                      # Match anything but a quote
            )                               # End capture group 1
            '                               # Match closing quote
        """

        return re.compile(combined_pattern, re.IGNORECASE | re.VERBOSE)

    return {k: compile_regex(v) for k, v in tags.items()}


def process_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Separate the 'tags' column into categories."""

    def find_category(category: str, df: pd.DataFrame) -> pd.DataFrame:
        """Find all tags that match the given category."""
        # Initialize new column
        column = []
        # Check each tag against each regex pattern
        for tag in df["tags"]:
            row = []

            for match in TAG_PATTERNS[category].finditer(tag):
                row.append(match.group(1))

            column.append(row)

        df[category] = column
        return df

    print("Category function defined.")

    for category in TAG_PATTERNS.keys():
        print(f"Finding {category}...")
        df = find_category(category, df)
        print(f"{category} found.")

    return df


# Run the function on the tags column
TAG_PATTERNS = load_tags(TAGS_FILE)
with st.spinner("Separating Tags..."):
    recipes = process_tags(recipes)
st.dataframe(recipes.head(20))
recipes.to_csv(CLEANED_RECIPES_CSV)


# Ingredient Mapping
# ingr = pd.read_pickle(PKL_FILE)
# st.dataframe(ingr.head(20))
# st.write(ingr.shape)
# st.write(ingr["id"].unique().shape)

print("Done!")

import ast
import csv
import re
from pathlib import Path
import yaml

import pandas as pd
import streamlit as st

# File paths
CSV_FILES = [
    Path("food-com-recipes/PP_recipes.csv"),
    Path("food-com-recipes/RAW_recipes.csv"),
]
PKL_FILE = Path("food-com-recipes/ingr_map.pkl")
CLEANED_RECIPES_CSV = Path("food-com-recipes/cleaned_recipes.csv")
TAGS_FILE = Path("food-com-recipes/tags.yaml")

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


st.write("Importing data...")
# save_first_50_lines_of_csv()
raw_recipes = pd.read_csv(CSV_FILES[1])
# pp_recipes = pd.read_csv(CSV_FILES[0])
st.dataframe(raw_recipes.head(50))


# ==============================================================================
def split_nutrition(df):
    """Split the 'nutrition' column into individual components."""

    # Split the 'nutrition' column into individual components
    dest_cols = [
        "calories",
        "total_fat_pdv",
        "sugar_pdv",
        "sodium_pdv",
        "protein_pdv",
        "saturated_fat_pdv",
        "carbohydrate_pdv",
    ]
    df[dest_cols] = pd.DataFrame(
        df["nutrition"].str.strip("[]").str.split(",").tolist(), dtype=float
    )
    # Drop the original 'nutrition' column
    # df = df.drop("nutrition", axis=1)

    return df


st.write("Splitting nutrition...")
recipes = split_nutrition(raw_recipes)
st.dataframe(recipes.head(50))

# df = raw_recipes["tags"]
# df.to_csv("tags.csv")

# ==============================================================================
st.write("Separating Tags...")


def load_tags(tags_file: Path) -> list:
    """Load the tags from the yaml file."""
    tags = yaml.safe_load(tags_file.open("r"))

    def compile_regex(patterns: list) -> re.Pattern:
        """Compile a list of regex patterns into a single pattern."""

        combined_pattern = f"""
            '
            (
                [^']*?
                (?:
                    {'|'.join(patterns)}
                )
                [^']*?
            )
            '
        """

        return re.compile(combined_pattern, re.IGNORECASE)

    return {k: compile_regex(v) for k, v in tags.items()}


def process_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Separate the 'tags' column into categories."""

    # Define regex patterns for each category
    TAG_PATTERNS = {
        "cuisine": re.compile(
            r"'italian|mexican|chinese|french|indian|american)[^']*?)'",
            re.IGNORECASE,
        ),
        "time": re.compile(r"'([^']*?(?:minutes?|hours?)[^']*?)'", re.IGNORECASE),
        "difficulty": re.compile(r"easy|medium|hard", re.IGNORECASE),
        "season": re.compile(
            r"'([^']*?(?:summer|winter|spring)[^']*?)'", re.IGNORECASE
        ),
        "equipment": re.compile(r"'([^']*?(?:grill|stove|oven)[^']*?)'", re.IGNORECASE),
        "course": re.compile(
            r"'([^']*?(?:main-dish|dessert|appetizer|side-dish|breakfast|lunch|dinner)[^']*?)'",
            re.IGNORECASE,
        ),
        "dietary_restriction": re.compile(
            r"'([^']*?(?:vegetarian|vegan|gluten-free|low-carb|low-fat)[^']*?)'",
            re.IGNORECASE,
        ),
        "ingredient": re.compile(
            r"'([^']*?(?:chicken|beef|pasta|tomato|cheese|bread|muffin)[^']*?)'",
            re.IGNORECASE,
        ),
        "event": re.compile(
            r"'([^']*?(?:christmas|thanksgiving|birthday|picnic)[^']*?)'", re.IGNORECASE
        ),
    }

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
recipes = process_tags(recipes)
st.dataframe(recipes.head(50))
recipes.to_csv(CLEANED_RECIPES_CSV)


# Ingredient Mapping
# ingr = pd.read_pickle(PKL_FILE)
# st.dataframe(ingr.head(50))
# st.write(ingr.shape)
# st.write(ingr["id"].unique().shape)

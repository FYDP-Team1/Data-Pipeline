import ast
import csv
import re
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# File paths
CSV_FILES = [
    Path("food-com-recipes/PP_recipes.csv"),
    Path("food-com-recipes/RAW_recipes.csv"),
]
PKL_FILE = Path("food-com-recipes/ingr_map.pkl")
CLEANED_RECIPES_CSV = Path("data/cleaned_recipes.csv")
TAGS_FILE = Path("food-com-recipes/tags.yaml")

# Set pd seed
np.random.seed(42)

raw_recipes = pd.read_csv(CSV_FILES[1])
print(f"Num Raw Recipes: {raw_recipes.shape}")
# pp_recipes = pd.read_csv(CSV_FILES[0])
# print(f"Num PP Recipes: {pp_recipes.shape}")


# ==============================================================================
def split_nutrition(df):
    print("Splitting Nutrition column")
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


recipes = split_nutrition(raw_recipes)
# ==============================================================================


def load_tags(tags_file: Path) -> list:
    """Load the tags from the yaml file."""
    print("Loading tags...")
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
    print("Processing tags...")

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

# Save the cleaned recipes
print(f"Num Cleaned Recipes: {recipes.shape}")
recipes.to_csv(CLEANED_RECIPES_CSV)

# Ingredient Mapping
ingr = pd.read_pickle(PKL_FILE)
print(f"Num Ingr: {ingr['id'].unique().shape}")

print("Done!")

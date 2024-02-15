import ast
from pathlib import Path

import polars as pl
import yaml

# File paths
RAW_RECIPES_FILES = Path("food-com-recipes/RAW_recipes.csv")
CLEANED_RECIPES_CSV = Path("data/recipes-1.csv")

TAGS_FILE = Path("data/tags.yaml")
TAG_REPLACEMENTS_FILE = Path("data/replacements.yaml")

# ==============================================================================


def split_nutrition(df: pl.DataFrame):
    print("Splitting Nutrition column")
    """Split the 'nutrition' column into individual components."""

    # Define individual components
    nutrition_categories = [
        "calories",
        "total_fat_pdv",
        "sugar_pdv",
        "sodium_pdv",
        "protein_pdv",
        "saturated_fat_pdv",
        "carbohydrate_pdv",
    ]

    # Split the nutrition column into individual columns
    df[nutrition_categories] = pl.DataFrame(map(ast.literal_eval, df["nutrition"]))

    # Drop the original 'nutrition' column
    df = df.drop("nutrition")

    return df


# ==============================================================================


def load_tags():
    """Load the tags from the yaml file."""
    print("Loading tags...")
    tags = yaml.safe_load(TAGS_FILE.open("r"))
    return tags


def process_tags(df: pl.DataFrame, tag_patterns: dict[str, list[str]]):
    """Separate the 'tags' column into categories."""
    print("Processing tags...")

    replacements = yaml.safe_load(TAG_REPLACEMENTS_FILE.open("r"))
    replacement_map = {item: cat for cat, lst in replacements.items() for item in lst}

    def find_category(pattern: list[str], df: pl.DataFrame):
        """Find all tags that match the given category."""
        # Initialize new column
        column = []
        # Check each tag against each regex pattern
        for tag in df["tags"]:
            tag = ast.literal_eval(tag)

            row = [
                replacement_map.get(match, match) for match in pattern if match in tag
            ]

            column.append(",".join(row) if row else None)

        return df.with_columns(pl.Series(category, column))

    print("Category function defined.")

    # Remove recipes with unwanted cuisines and courses
    for category in ["cuisine", "course"]:
        print(f"Finding {category}...")
        df = find_category(tag_patterns[category], df)
        df = df.filter(pl.col(category).is_not_null())
        print(f"{category} found.")

    for category, pattern in tag_patterns.items():
        if category in ["cuisine", "course"]:
            continue

        print(f"Finding {category}...")
        df = find_category(pattern, df)
        print(f"{category} found.")

    # Remove rows that have not been assigned any tags in the category columns
    print("Removing rows with no assigned tags...")
    category_columns = tag_patterns.keys()
    df = df.filter(~pl.all_horizontal(pl.col(category_columns).is_null()))
    print("Rows removed.")

    # Remove the original 'tags' column
    df = df.drop("tags")

    return df


# ==============================================================================


def clean_recipe_text(df: pl.DataFrame):
    """Clean the text columns: name, description, steps"""

    # Remove extra spaces and newlines
    # Escape double quotes
    return df.with_columns(
        pl.col("name").str.replace_all(r"\s+", " ").str.replace_all('"', "'"),
        pl.col("description")
        .str.replace_all(r"[\n\s]+", " ")
        .str.replace_all('"', "'"),
        pl.col("steps").str.replace_all(r"\s+", " ").str.replace_all('"', "'"),
    )


# ==============================================================================


def sample_recipes(sampled_recipes: pl.DataFrame):
    sampled_cuisines = pl.DataFrame(None, schema=sampled_recipes.schema)
    for cuisine, grp in sampled_recipes.group_by("cuisine"):
        if grp.shape[0] < 50:
            sample = grp
        else:
            sample = grp.sample(n=50, seed=40404, with_replacement=False)
        sampled_cuisines = sampled_cuisines.vstack(sample)
    sampled_recipes = sampled_cuisines.sample(
        n=200,
        seed=40404,
        with_replacement=False,
        shuffle=True,
    )
    for cuisine, grp in sampled_recipes.group_by("cuisine"):
        print(f"{cuisine}: {grp.shape}")
    return sampled_recipes


# ==============================================================================


if __name__ == "__main__":
    recipes = (
        pl.scan_csv(RAW_RECIPES_FILES).drop(["contributor_id", "submitted"]).collect()
    )
    print(f"Raw Recipes: {recipes.shape}")

    # Clean the recipes
    tag_patterns = load_tags()
    recipes = process_tags(recipes, tag_patterns)
    print(f"Cleaned Recipes: {recipes.shape}")

    recipes = sample_recipes(recipes)
    print(f"Sampled Recipes: {recipes.shape}")

    recipes = split_nutrition(recipes)
    recipes = clean_recipe_text(recipes)

    # Save the recipes
    recipes.write_csv(CLEANED_RECIPES_CSV)

    print("Done!")

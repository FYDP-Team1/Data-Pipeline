import ast
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

st.set_page_config(layout="wide")

RECIPE_FILE = Path("food-com-recipes/cleaned_recipes.csv")
TAGS_FILE = Path("food-com-recipes/tags.yaml")


def load_tags(tags_file: Path) -> dict:
    """Load the tags from the yaml file."""
    tags = yaml.safe_load(tags_file.open("r"))
    tags = {k: set(v) for k, v in tags.items()}
    return tags


with st.spinner("Loading recipes..."):
    recipes_df = pd.read_csv(RECIPE_FILE, index_col=0)

# st.dataframe(recipes_df.head(50))

# Create user form that has dropdowns for some tags
with st.form(key="preferences_form"):
    st.header("Select your preferences:")

    # Load Tags
    tags = load_tags(TAGS_FILE)

    # Dietary Restrictions Dropdown
    dietary_restrictions = set(
        st.multiselect(
            label="Dietary Restrictions", options=tags["dietary_restrictions"]
        )
    )

    # Cuisine Dropdown
    cuisine = set(st.multiselect(label="Cuisine", options=tags["cuisine"]))

    # Submit button
    submit_button = st.form_submit_button(label="Submit")


# Button clicked, filter recipes
if submit_button:
    with st.spinner("Filtering recipes..."):
        # If selections are empty, set to all
        cuisine = tags["cuisine"] if not cuisine else cuisine
        dietary_restrictions = (
            tags["dietary_restrictions"]
            if not dietary_restrictions
            else dietary_restrictions
        )

        # Check each index for tags
        dietary_restrictions_filter = pd.Series(
            [
                any(tag in dietary_restrictions for tag in ast.literal_eval(tags))
                for tags in recipes_df["dietary_restrictions"]
            ]
        )
        cuisine_filter = pd.Series(
            [
                any(tag in cuisine for tag in ast.literal_eval(tags))
                for tags in recipes_df["cuisine"]
            ]
        )

        # Filter recipes
        filtered_recipes = recipes_df[dietary_restrictions_filter & cuisine_filter]

    # Display 20 random filtered recipes
    if len(filtered_recipes) == 0:
        st.header("No recipes found!")
        st.stop()
    if len(filtered_recipes) < 20:
        filter_number = len(filtered_recipes)
    else:
        filter_number = 20

    st.header(f"Here are {filter_number} recipes you might like:")
    st.dataframe(filtered_recipes.sample(filter_number))

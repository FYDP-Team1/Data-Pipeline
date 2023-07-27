import streamlit as st
import pandas as pd
from pathlib import Path
import yaml

st.set_page_config(layout="wide")

TAGS_FILE = Path("food-com-recipes/tags.yaml")


def load_tags(tags_file: Path) -> list:
    """Load the tags from the yaml file."""
    tags = yaml.safe_load(tags_file.open("r"))


# Create user form that has dropdowns for dietary_restrictions, cuisine

import ast
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

RECIPE_FILE = Path("food-com-recipes/RAW_recipes.csv")
TAGS_FILE = Path("food-com-recipes/all_tags.yaml")

# wide display
st.set_page_config(layout="wide")

prog_bar = st.progress(20, "Loading Recipe Data...")
recipes = pd.read_csv(RECIPE_FILE)

prog_bar.progress(40, "Extracting Tags...")
# Put all tags into a set after converting from string to list
tags = {
    tag for tag_list in recipes["tags"] for tag in ast.literal_eval(tag_list) if tag
}

prog_bar.progress(50, "Creating DataFrame...")
data = pd.DataFrame(tags, columns=["tags"])

columns = {
    "tags": st.column_config.TextColumn(required=True, validate="^[a-z-]+$"),
    "season": st.column_config.CheckboxColumn(),
    "event": st.column_config.CheckboxColumn(),
    "time": st.column_config.CheckboxColumn(),
    "difficulty": st.column_config.CheckboxColumn(),
    "equipment": st.column_config.CheckboxColumn(),
    "cooking_method": st.column_config.CheckboxColumn(),
    "cuisine": st.column_config.CheckboxColumn(),
    "course": st.column_config.CheckboxColumn(),
    "dietary_restrictions": st.column_config.CheckboxColumn(),
    "key_ingredient": st.column_config.CheckboxColumn(),
    "dish": st.column_config.CheckboxColumn(),
}

# Load tags.yaml if it exists
prog_bar.progress(60, "Loading Tag Categories File...")
with TAGS_FILE.open("r") as f:
    saved_tags: dict = yaml.safe_load(f)

prog_bar.progress(70, "Adding New Columns...")
# Add new columns with all unsaved values set to False
for col in columns:
    if col != "tags":
        data[col] = 0

prog_bar.progress(80, "Assigning Saved Tags...")
# Set saved values to True
for row in data.index:
    for col in columns:
        if col != "tags":
            if col in saved_tags and data["tags"][row] in saved_tags[col]:
                saved_tags[col].remove(data["tags"][row])
                data[col][row] = 1

prog_bar.progress(90, "Appending Missing Tags...")
# Add remaining saved values to the end of the dataframe
reamining_saved_tags = [itm for lst in saved_tags.values() for itm in lst]
new_data = []
for tag in reamining_saved_tags:
    row = {"tags": tag}
    for col in columns:
        if col != "tags":
            row[col] = 1 if tag in saved_tags[col] else 0
    new_data.append(row)

data = pd.concat([data, pd.DataFrame(new_data)], ignore_index=True)

prog_bar.progress(100, "Done!")
edited = st.data_editor(
    data,
    hide_index=False,
    use_container_width=True,
    column_config=columns,
    height=1300,
    num_rows="dynamic",
)

if st.button("Save", type="primary"):
    # Replace True with tag name
    for col in columns:
        if col != "tags":
            for tag_index in edited[edited[col] == 1].index:
                edited[col][tag_index] = edited["tags"][tag_index]

    # Create dict without 'tags' column, ignore NaN values
    edited = edited.drop("tags", axis=1)
    data_dict = edited.to_dict("list")
    data_dict = {k: list({x for x in v if x != 0}) for k, v in data_dict.items()}

    with TAGS_FILE.open("w") as f:
        yaml.safe_dump(data_dict, f)
    st.success("Saved!")

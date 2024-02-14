from pathlib import Path
from docx import Document
import yaml
import re

TAGS_IN_FILE = Path("data/all_tags.yaml")
TAGS_OUT_FILE = Path("data/tags.yaml")
REPLACEMENTS_FILE = Path("data/replacements.yaml")

remove_leading_spaces_and_hyphens = re.compile(r"^\s*-\s*")


def extract_strikethrough_text(doc_path):
    doc = Document(doc_path)
    strikethrough_text = []

    for para in doc.paragraphs:
        for run in para.runs:
            if run.font.strike:
                strikethrough_text.append(
                    remove_leading_spaces_and_hyphens.sub("", run.text)
                )

    return strikethrough_text


strikethrough_tags = extract_strikethrough_text("data/Tags Categorization.docx")

# Load the YAML file
data = yaml.safe_load(TAGS_IN_FILE.open("r"))

# Remove strikethrough tags
cleaned_data = {
    cat: [tag for tag in lst if tag not in strikethrough_tags]
    for cat, lst in data.items()
}

replacements = yaml.safe_load(REPLACEMENTS_FILE.open("r"))

# Replace tags
for replace, find_list in replacements.items():
    for find in find_list:
        for category, lst in cleaned_data.items():
            cleaned_data[category] = [replace if tag == find else tag for tag in lst]

# Remove duplicates
filtered_data = {k: sorted(set(v)) for k, v in cleaned_data.items()}

# Write the cleaned data back to the YAML file
yaml.dump(filtered_data, TAGS_OUT_FILE.open("w"))

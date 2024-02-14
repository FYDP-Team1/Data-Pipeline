import json
import re
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup
from statistics import mean

from mixed_fractions import Mixed

CLEANED_RECIPES_CSV = Path("data/cleaned_recipes.csv")
INGREDIENTS_CSV = Path("data/recipes_ingredients.csv")


def recipe_link(name, r_id):
    name = name.lower().replace(" ", "-")
    return f"https://www.food.com/recipe/{name}-{r_id}?scale=1&units=us"


def clean_text(text):
    text = re.sub(r"\s+", " ", text.strip())
    text = re.sub(r"<a[^>]*>|</a>", "", text)
    return text


def clean_quantity(text):
    if text == "":
        return ""

    def fraction_to_float(text):
        text = text.replace("⁄", "/")  # Replace special character with '/'
        return float(Mixed(text))

    text = re.sub(r"\s+", " ", text.strip())
    if "-" in text:
        text = text.split("-")
        number = mean([fraction_to_float(t) for t in text])
    else:
        number = fraction_to_float(text)
    return number


def scrape_ingredients(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    ingredients = soup.find_all("ul", class_="ingredient-list").pop()
    output = {}
    for ingredient in ingredients.find_all("li"):
        try:
            quantity = ingredient.find("span", class_="ingredient-quantity").text
            name = ingredient.find("span", class_="ingredient-text").text
            output[clean_text(name)] = clean_quantity(quantity)
        except AttributeError:
            pass
    return json.dumps(output)


if __name__ == "__main__":
    recipes = pl.scan_csv(CLEANED_RECIPES_CSV).select(["name", "id"]).collect()

    print("Generating links...")
    recipes = recipes.with_columns(
        pl.struct(["name", "id"])
        .map_elements(lambda x: recipe_link(x["name"], x["id"]), strategy="threading")
        .alias("link")
    )

    print("Scraping ingredients...")
    recipes = recipes.with_columns(
        pl.col("link")
        .map_elements(lambda x: scrape_ingredients(x), strategy="threading")
        .alias("ingredients")
    )

    print("Saving...")
    recipes.sort("id").write_csv(INGREDIENTS_CSV)

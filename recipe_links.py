from pathlib import Path

import polars as pl

CLEANED_RECIPES_CSV = Path("data/cleaned_recipes.csv")
LINKS_CSV = Path("data/recipes_links.csv")


def recipe_link(name, r_id):
    name = name.lower().replace(" ", "-")
    return f"https://www.food.com/recipe/{name}-{r_id}"


if __name__ == "__main__":
    recipes = pl.scan_csv(CLEANED_RECIPES_CSV).select(["name", "id"]).collect()

    recipes = recipes.with_columns(
        pl.struct(["name", "id"])
        .map_elements(lambda x: recipe_link(x["name"], x["id"]))
        .alias("link")
    )

    recipes.write_csv(LINKS_CSV)

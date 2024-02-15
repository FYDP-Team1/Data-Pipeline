from io import TextIOWrapper
from pathlib import Path

import polars as pl
import numpy as np

RECIPES_CSV = Path("data/recipes-1.csv")
INGREDIENTS_CSV = Path("data/ingredients-2.csv")
SQL_FILE = Path("data/seed.sql")


def get_recipes_table(recipes: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Create the recipes table."""
    df = recipes.select(
        [
            pl.col("id"),
            pl.col("name"),
            pl.col("description"),
            pl.col("minutes").alias("cooking_minutes"),
            # pl.col("cost"),
            pl.col("n_steps"),
            pl.col("steps"),
            pl.col("n_ingredients"),
            pl.col("calories"),
            pl.col("total_fat_pdv"),
            pl.col("sugar_pdv"),
            pl.col("sodium_pdv"),
            pl.col("protein_pdv"),
            pl.col("saturated_fat_pdv"),
            pl.col("carbohydrate_pdv"),
            pl.col("cooking_method"),
            pl.col("course"),
            pl.col("difficulty"),
            pl.col("dish"),
            pl.col("equipment"),
            pl.col("event"),
            pl.col("key_ingredient"),
            pl.col("season"),
        ]
    )

    def rand_cost(seed):
        return round(((30 * np.random.default_rng(seed).random()) + 10), 2)

    # Add dummy cost data
    df = df.with_columns(
        pl.col("id")
        .map_elements(
            lambda x: rand_cost(x),
            return_dtype=pl.Float64,
            strategy="threading",
        )
        .alias("cost")
    )
    return {"recipes": df}


def get_cuisines_table(
    recipes: pl.DataFrame, category: str, association: str
) -> dict[str, pl.DataFrame | dict]:
    """Create the category and recipe_category tables."""

    category_df = recipes.select(
        pl.col(category)
        .map_elements(lambda x: x.split(","), strategy="threading")
        .explode()
    ).unique()
    category_df = category_df.with_row_index("id", offset=1)

    cat_dict = category_df.to_dict(as_series=False)
    recipe_dict = recipes.select(pl.col("id"), pl.col(category)).to_dicts()
    recipe_category = [
        {
            f"{category}_id": c_id,
            "recipe_id": recipe["id"],
        }
        for c_id, cat in zip(cat_dict["id"], cat_dict[category])
        for recipe in recipe_dict
        if cat and recipe[category] and cat in recipe[category]
    ]

    return {
        category: category_df,
        association: recipe_category,
    }


def write_sql(data: pl.DataFrame | list, table_name: str, file: TextIOWrapper):
    """Write the data to a SQL init file."""
    print(f"Writing table {table_name}...")

    if isinstance(data, list):
        cols = data[0].keys()
        iterator = iter(data)
    elif isinstance(data, pl.DataFrame):
        cols = data.columns
        iterator = data.iter_rows(named=True)
    else:
        raise ValueError("The data must be a list or a Polars DataFrame.")

    cols_string = ", ".join(cols)
    for row in iterator:
        vals_string = ", ".join([f'"{str(row[col])}"' for col in cols])
        file.write(
            f"INSERT INTO {table_name} ({cols_string}) VALUES ({vals_string});\n"
        )
    print(f"{table_name} table written.")


if __name__ == "__main__":
    recipes = pl.read_csv(RECIPES_CSV)

    # Split the dataframe into individual tables
    #   Independant tables: recipes, ingredients, cuisines, dietary_restrictions
    #   Dependant tables: recipe_ingredients, recipe_cuisines, recipe_restrictions
    database = {}
    database.update(get_recipes_table(recipes))
    database.update(get_cuisines_table(recipes, "cuisine", "recipe_cuisines"))
    database.update(
        get_cuisines_table(recipes, "dietary_restrictions", "recipe_restrictions")
    )

    # Write the SQL file
    with SQL_FILE.open("w") as file:
        for table, data in database.items():
            file.write(f"-- {table} table\n")
            write_sql(data, table, file)
            file.write("\n\n")
    print("SQL file written.")

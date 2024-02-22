import ast
import json
from pathlib import Path
import pint

import polars as pl

INPUT_INGREDIENTS_CSV = Path("data/ingredients-1.csv")
INPUT_RECIPES_CSV = Path("data/recipes-1.csv")
PRICELIST_CSV = Path("data/ingredient-pricelist.csv")
OUTPUT_INGREDIENTS_CSV = Path("data/ingredients-2.csv")
OUTPUT_INGREDIENT_RECIPE_CSV = Path("data/ingredient-recipe-2.csv")
OUTPUT_RECIPES_CSV = Path("data/recipes-2.csv")

UREG = pint.UnitRegistry(case_sensitive=False)


def get_recipe_ingredients(
    recipes_file: Path, ingredients_file: Path
) -> list[dict[str, str]]:
    """Get the recipe ingredients."""
    # Read the CSV files
    ingredients = (
        pl.scan_csv(ingredients_file)
        .select(["id", "ingredients"])
        .with_columns(
            [
                pl.col("id").alias("recipe_id"),
                pl.col("ingredients").alias("ingredient_labels_quantities"),
            ]
        )
        .collect()
    )
    recipes = (
        pl.scan_csv(recipes_file)
        .select(["id", "ingredients"])
        .with_columns(
            [
                pl.col("id").alias("recipe_id"),
                pl.col("ingredients").alias("ingredient_names"),
            ]
        )
        .collect()
    )

    # Merge the DataFrames
    recipe_ingredients = recipes.join(ingredients, on="recipe_id", how="inner").select(
        [
            "recipe_id",
            "ingredient_labels_quantities",
            "ingredient_names",
        ]
    )

    # Convert to list of dictionaries
    recipe_ingredients = recipe_ingredients.to_dicts()

    # Convert ingridients strings to lists and dicts
    for recipe in recipe_ingredients:
        temp = recipe["ingredient_names"]
        recipe["ingredient_names"] = ast.literal_eval(temp)

        temp = recipe["ingredient_labels_quantities"]
        recipe["ingredient_labels_quantities"] = json.loads(temp)

    return recipe_ingredients


def get_ingredient_product_map(
    file: Path,
) -> dict[str, dict[str, float | pint.Quantity]]:
    """Get a mapping of ingredient names to product prices, quantities, and units."""

    pricelist = pl.read_csv(file).to_dicts()

    return {
        name.strip("'"): {
            "quantity": pint.Quantity(ingredient["quantity"], ingredient["unit"]),
            "price": ingredient["price"],
        }
        for ingredient in pricelist
        for name in ingredient["ingredient"].split("|")
    }


def extract_values(ingredient: str, mapping: dict) -> dict[str, str | pint.Quantity]:
    """Extract the unit from an ingredient string."""
    # TODO: individual if statements to match strings?
    return ingredient.split(" ")[0:1]


def calculate_recipe_cost(
    product_map: dict[str, dict[str, float | pint.Quantity]],
    ingredients: list[pint.Quantity],
):
    """Calculate the cost of a recipe."""
    total = 0.0
    for ingredient in ingredients:
        try:
            ratio = (
                ingredient["quantity"] / product_map[ingredient["name"]]["quantity"]
            ).to_reduced_units()
            if ratio.units.dimensionless:
                total += ratio.magnitude * product_map[ingredient["name"]]["price"]
            else:
                raise pint.errors.DimensionalityError(
                    ingredient["quantity"].units,
                    product_map[ingredient["name"]]["quantity"].units,
                )
        except pint.errors.DimensionalityError as e:
            print(
                f"Dimensionality mismatch: {ingredient['quantity'].units} != {product_map[ingredient['name']]['quantity'].units}\n{e}"
            )

    return total


def combine_ingredients(
    recipe_ingredients: list[dict[str, str | list[str] | dict[str, str | float]]],
    product_map: dict[str, dict[str, float | pint.Quantity]],
) -> tuple[
    list[dict[str, str | int]],
    list[dict[str, str | int | float]],
    dict[int, float],
]:
    """Get a list of ingredient names, and a mapping of recipes and ingredients."""
    recipe_ingredient_maps = {}
    for recipe in recipe_ingredients:
        labels = list(recipe["ingredient_labels_quantities"].keys())
        ingredient_map = {
            name: [
                extract_values(label, recipe["ingredient_labels_quantities"])
                for label in labels
                if name in label
            ]
            for name in recipe["ingredient_names"]
        }
        recipe_ingredient_maps[recipe["recipe_id"]] = ingredient_map

    ingredient_to_id = {
        ingredient: i for i, ingredient in enumerate(product_map.keys())
    }

    recipe_cost = {
        recipe_id: calculate_recipe_cost(
            product_map,
            [
                ingr["quantity"]
                for ingr_lst in ingredient_map.values()
                for ingr in ingr_lst
            ],
        )
        for recipe_id, ingredient_map in recipe_ingredient_maps.items()
    }

    ingredient_recipe_relations = [
        {
            "recipe_id": recipe_id,
            "ingredient_id": ingredient_to_id[name],
            "label": label_dict["label"],
            "quantity": (
                float(label_dict["quantity"].magnitude)
                if label_dict["quantity"]
                else 0.0
            ),
            "unit": str(label_dict["quantity"].units) if label_dict["quantity"] else "",
        }
        for recipe_id, ingredient_map in recipe_ingredient_maps.items()
        for name, ingredient_list in ingredient_map.items()
        for label_dict in ingredient_list
    ]

    ingredients = [{"id": v, "name": k} for k, v in ingredient_to_id.items()]

    return ingredients, ingredient_recipe_relations, recipe_cost


def save_recipe_cost(input: Path, output: Path, recipe_cost: dict[int, float]):
    """Update the recipe table with calcuated cost."""
    pl.read_csv(input).with_columns(
        pl.col("id").replace(recipe_cost).alias("cost")
    ).write_csv(output)


if __name__ == "__main__":
    recipe_ingredients = get_recipe_ingredients(
        INPUT_RECIPES_CSV, INPUT_INGREDIENTS_CSV
    )
    product_map = get_ingredient_product_map(PRICELIST_CSV)

    ingredients, ingredient_recipe, recipe_cost = combine_ingredients(
        recipe_ingredients, product_map
    )

    save_recipe_cost(INPUT_RECIPES_CSV, OUTPUT_RECIPES_CSV, recipe_cost)

    pl.DataFrame(ingredients).write_csv(OUTPUT_INGREDIENTS_CSV)
    pl.DataFrame(ingredient_recipe).write_csv(OUTPUT_INGREDIENT_RECIPE_CSV)

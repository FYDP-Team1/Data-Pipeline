import ast
import json
import re
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
            "quantity": UREG.Quantity(ingredient["quantity"], ingredient["unit"]),
            "price": ingredient["price"],
        }
        for ingredient in pricelist
        for name in ingredient["ingredient"].split("|")
    }


def extract_values(ingredient: str, mapping: dict) -> dict[str, str | pint.Quantity]:
    """Extract the unit from an ingredient string."""

    def format_output(quantity: pint.Quantity | tuple[float, str]):
        if isinstance(quantity, tuple):
            quantity = UREG.Quantity(quantity[0] if quantity[0] else 1, quantity[1])
        if isinstance(quantity, pint.Quantity):
            return {
                "label": ingredient,
                "quantity": quantity,
            }
        raise ValueError(f"Quantity not found: {quantity}")

    # Check for a quantity in parentheses
    if ingredient.startswith("("):
        qty_str = re.search(r"\((.*?)\)", ingredient).group(1)
        try:
            qty = UREG.Quantity(qty_str)
            return format_output(qty)
        except pint.errors.PintError as e:
            print(f"Quantity not found: {qty_str}\n{type(e)}:{e}\n")

    # Pre-defined units
    if "to taste" in ingredient:
        return format_output((mapping[ingredient], "to_taste"))

    if ingredient in [
        "salt and black pepper",
        "salt and pepper",
        "salt",
        "pepper",
        "vegetable oil cooking spray, for pan",
        "italian seasoning",
    ]:
        return format_output((mapping[ingredient], "to_taste"))

    # Check for a unit in the ingredient string
    words = ingredient.split(" ")
    for word in words:
        try:
            return format_output(UREG.Quantity(mapping[ingredient], word))
        except Exception:
            continue

    print(f"Unit not found, assigning 'each': {ingredient}")
    return format_output((mapping[ingredient], "each"))


def calculate_recipe_cost(
    product_map: dict[str, dict[str, float | pint.Quantity]],
    ingredients: list[dict[str, str | pint.Quantity]],
):
    """Calculate the cost of a recipe."""
    total = 0.0
    for ingredient in ingredients:
        product = product_map.get(ingredient["name"])
        quantity = ingredient["quantity"]

        # Check if the units are NOT of the same type (i.e. mass and volume, or mass and count, etc.)
        if not quantity.is_compatible_with(product["quantity"]):
            # TODO
            pass

        try:
            ratio = (quantity / product["quantity"]).to_reduced_units()
            if ratio.units.dimensionless:
                total += ratio.magnitude * product["price"]
            else:
                raise pint.errors.DimensionalityError(
                    quantity.u,
                    product["quantity"].u,
                    quantity.u.dimensionality,
                    product["quantity"].u.dimensionality,
                )
        except pint.errors.DimensionalityError as e:
            print(f"{type(e)}:{e}")

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
                {"name": ingr_name, "quantity": ingr["quantity"]}
                for ingr_name, ingr_lst in ingredient_map.items()
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
    # Define additional units (estimated)
    UREG.define("each = count")
    UREG.define("bunch = 150 * gram")
    UREG.define("pinch = 1/16 * teaspoon = sprinkle")
    UREG.define("dash = 1/8 * teaspoon = to_taste")
    UREG.define("handful = 1/2 * cup")

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

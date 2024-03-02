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
            "quantity": validate_quantity(
                f'{ingredient["quantity"]} {ingredient["unit"]}'
            ),
            "price": ingredient["price"],
        }
        for ingredient in pricelist
        for name in ingredient["ingredient"].split("|")
    }


def validate_quantity(qty_str: str) -> pint.Quantity:
    quantity = UREG.Quantity(qty_str)

    if "gauss" in str(quantity.units):
        quantity = UREG.Quantity(
            quantity.magnitude, str(quantity.units).replace("gauss", "gram")
        )

    if quantity.dimensionless or (
        len(quantity.dimensionality) == 1
        and (
            (
                "[mass]" in quantity.dimensionality
                and quantity.dimensionality["[mass]"] == 1
            )
            or (
                "[length]" in quantity.dimensionality
                and quantity.dimensionality["[length]"] == 3
            )
        )
    ):
        return quantity
    else:
        raise ValueError(f"Invalid units: {quantity.units}")


def format_ingredient_label_quantity(
    label: str, quantity: pint.Quantity | tuple[float, str]
):
    if isinstance(quantity, tuple):
        quantity = UREG.Quantity(quantity[0] if quantity[0] else 1, quantity[1])
    if isinstance(quantity, pint.Quantity):
        try:
            return {
                "label": label,
                "quantity": validate_quantity(quantity),
            }
        except ValueError as e:
            print(f"{type(e)}:{e}")
            return None
    raise ValueError(f"Quantity not found: {quantity}")


def extract_values(ingredient: str, mapping: dict) -> dict[str, str | pint.Quantity]:
    """Extract the unit from an ingredient string."""
    # Check for a quantity in parentheses
    if ingredient.startswith("("):
        qty_str = re.search(r"\((.*?\d.*?)\)", ingredient).group(1)
        try:
            qty = UREG.Quantity(qty_str)
            output = format_ingredient_label_quantity(ingredient, qty)
            if output:
                return output
        except pint.errors.PintError as e:
            print(f"Quantity not found: {qty_str}\n{type(e)}:{e}\n")

    # Check for a unit in the ingredient string
    words = ingredient.split(" ")
    for word in words:
        try:
            qty = UREG.Quantity(mapping[ingredient], word)
            output = format_ingredient_label_quantity(ingredient, qty)
            if output:
                return output
        except Exception:
            continue

    # Manually-defined units
    if "to taste" in ingredient or ingredient in [
        "salt and black pepper",
        "salt and pepper",
        "salt",
        "pepper",
        "vegetable oil cooking spray, for pan",
        "italian seasoning",
    ]:
        output = format_ingredient_label_quantity(
            ingredient, (mapping[ingredient], "to_taste")
        )
        if output:
            return output

    print(f"Unit not found, assigning 'each': {ingredient}")
    output = format_ingredient_label_quantity(ingredient, (mapping[ingredient], "each"))
    if output:
        return output


def reconsile_incomepatible_units(
    quantity: pint.Quantity, product_quantity: pint.Quantity, name: str
):
    if quantity.dimensionless:
        if (
            "[length]" in product_quantity.dimensionality
            and product_quantity.dimensionality["[length]"] == 3
        ):
            return UREG.Quantity(
                2 if quantity.magnitude == 1 else quantity.magnitude,
                "tablespoon",
            )

        if "[mass]" in product_quantity.dimensionality:
            # quantity * unit product quantity
            return quantity * product_quantity / product_quantity.magnitude

    if (
        "[length]" in quantity.dimensionality
        and quantity.dimensionality["[length]"] == 3
    ):
        if "[mass]" in product_quantity.dimensionality:
            # Using water density = 1 g/ml
            return quantity * UREG.Quantity(1, "gram/milliliter")
        elif product_quantity.dimensionless:
            if "lemon" in name:
                return quantity / UREG.Quantity(3, "tablespoon")
            if "ginger" in name:
                return quantity / UREG.Quantity(2, "cup")
            if "onion" in name:
                return quantity / UREG.Quantity(1, "cup")
            if any(
                v in name
                for v in [
                    "sweet pepper",
                    "bell pepper",
                    "red pepper",
                    "green pepper",
                    "yellow pepper",
                    "capsicum",
                ]
            ):
                return quantity / UREG.Quantity(1.5, "cup")
            if "garlic" in name:
                return quantity / UREG.Quantity(100, "milliliter")
            if any(
                v in name
                for v in [
                    "lettuce",
                    "cabbage",
                ]
            ):
                return quantity / UREG.Quantity(100, "milliliter")
            if "lime" in name:
                return quantity / UREG.Quantity(2, "tablespoon")
            if "avocado" in name:
                return quantity / UREG.Quantity(1, "cup")
            if any(
                v in name
                for v in [
                    "tortillas",
                    "corn tortilla",
                ]
            ):
                return quantity / UREG.Quantity(35, "milliliters")
            if "jalapeno pepper" in name:
                return quantity / UREG.Quantity(2.5, "tablespoon")
            if "seedless watermelon" in name:
                return quantity / UREG.Quantity(11, "cup")
            
           

    if "[mass]" in quantity.dimensionality:
        if (
            "[length]" in product_quantity.dimensionality
            and product_quantity.dimensionality["[length]"] == 3
        ):
            # Using water density = 1 g/ml
            return quantity * UREG.Quantity(1, "milliliter/gram")
        elif product_quantity.dimensionless:
            if "onion" in name:
                return quantity / UREG.Quantity(200, "gram")
            if "baby bok choy" in name:
                return quantity / UREG.Quantity(50, "gram")
            if "watermelon" in name:
                return quantity / UREG.Quantity(9, "kilogram")
            if "eggplant" in name:
                return quantity / UREG.Quantity(200, "gram")
            if "kefalotiri" in name:
                return quantity / UREG.Quantity(100, "gram")
            if any(
                v in name
                for v in [
                    "sweet pepper",
                    "bell pepper",
                    "red pepper",
                    "green pepper",
                    "yellow pepper",
                    "capsicum",
                ]
            ):
                return quantity / UREG.Quantity(100, "gram")
            if "garlic" in name:
                return quantity / UREG.Quantity(40, "gram")
            if "lettuce leaf" in name:
                return quantity / UREG.Quantity(8, "gram")
            

    # Any remaining cases
    print(
        f"Unable to reconcile units for '{name}': '{quantity}'='{quantity.dimensionality}' and '{product_quantity}'='{product_quantity.dimensionality}'"
    )
    pass


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
            quantity = reconsile_incomepatible_units(
                quantity, product["quantity"], ingredient["name"]
            )

        try:
            ratio = (quantity / product["quantity"]).to_reduced_units()
            if ratio.units.dimensionless:
                total += ratio.magnitude * product["price"]
            else:
                raise pint.errors.DimensionalityError(
                    quantity.u,
                    product["quantity"].u,
                    quantity.dimensionality,
                    product["quantity"].dimensionality,
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
    UREG.define("splash = 2 * tablespoon")

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

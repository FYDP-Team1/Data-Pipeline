import json
from pathlib import Path
from dotenv import dotenv_values

import polars as pl
import requests

ENV_FILE = Path(".env")
INGREDIENTS_CSV = Path("data/ingredients-1.csv")
PRICE_CSV = Path("data/ingredient-2.csv")


def run_request(search_term: str, api_key: str):
    """Make a request to the BlueCart API."""
    params = {
        "api_key": api_key,
        "search_term": search_term,
        "type": "search",
    }
    api_result = requests.get("https://api.bluecartapi.com/request", params)

    # Check if the request was successful
    if api_result.status_code != 200:
        print(f"API request failed with status code {api_result.status_code}")
        print(api_result.text)
        return None

    # Load the JSON response into a dictionary
    return api_result.json()


if __name__ == "__main__":
    ##### PAY US$60 IF MAKING > 1200 requests
    # api_key = dotenv_values(ENV_FILE).get("BLUECART_API_KEY")

    result = run_request("fennel seeds", api_key)

    # Extract the name and price from the JSON response
    data = [
        {
            "name": item["product"]["title"],
            "item_id": item["product"][
                "item_id"
            ],  # Pass item_id into a "type=product" request to get product-level data about the product.
            "price": item["offers"]["primary"]["price"],
        }
        for item in result["search_results"]
    ]

    # output the data to a csv file
    with open(PRICE_CSV, "w") as file:
        file.write("item_id, name, price\n")
        for row in data:
            file.write(f"'{row['item_id']}', '{row['name']}', '{row['price']}'\n")

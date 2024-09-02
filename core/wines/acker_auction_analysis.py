import asyncio
import os
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from core.wines.wine_searcher import process_wine_list


def load_catalog_data(file_path: str) -> pd.DataFrame:
    """Load the catalog data from an Excel file."""
    logger.info(f"Loading catalog data from {file_path}")
    return pd.read_excel(file_path, sheet_name="qryCatalogExcel")


def extract_wine_names(catalog_df: pd.DataFrame) -> pd.DataFrame:
    """Extract and combine Vintage, Producer, WineName, and Designation fields."""
    logger.info("Extracting wine names")

    def combine_wine_name(row):
        parts = [
            str(row["Vintage"]),
            row["Producer"] if pd.notnull(row["Producer"]) else "",
            row["WineName"].strip(),
            row["Designation"] if pd.notnull(row["Designation"]) else "",
        ]
        return " ".join(part for part in parts if part)

    catalog_df["FullWineNameWithProducer"] = catalog_df.apply(combine_wine_name, axis=1)

    return catalog_df


def get_format_multipliers() -> Dict[str, float]:
    """Define multipliers for each bottle format."""
    return {
        "bottle": 1,
        "magnum": 2,
        "half-bottle": 0.5,
        "jeroboam": 4,
        "double magnum": 4,
        "methuselah": 8,
        "nebuchadnezzar": 20,
        "imperial": 8,
        "6 liter": 8,
    }


def calculate_unit_price(row: pd.Series, format_multipliers: Dict[str, float]) -> float:
    """Calculate unit price based on the bottle format and quantity."""
    format_multiplier = format_multipliers.get(row["BottleName"].lower(), 1)
    total_units = row["Quantity"] * format_multiplier
    return row["Low"] / total_units if total_units > 0 else row["Low"]


def process_catalog_data(
    catalog_df: pd.DataFrame, search_results_df: pd.DataFrame
) -> pd.DataFrame:
    """Process catalog data and merge with search results."""
    logger.info("Processing catalog data and merging with search results")
    joined_df = catalog_df.merge(
        search_results_df,
        left_on="FullWineNameWithProducer",
        right_on="query",
        how="inner",
    )

    format_multipliers = get_format_multipliers()
    joined_df["auction_unit_price"] = joined_df.apply(
        lambda row: calculate_unit_price(row, format_multipliers), axis=1
    )

    # Calculate the new auction_on_hand_unit_price
    joined_df["auction_on_hand_unit_price"] = joined_df["auction_unit_price"] * 1.27 + 7

    # Calculate discount percentage using auction_on_hand_unit_price
    joined_df["discount_percentage"] = (
        (joined_df["min_price"] - joined_df["auction_on_hand_unit_price"])
        / joined_df["min_price"]
    ) * 100
    return joined_df.sort_values(by="discount_percentage", ascending=False)


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns in the DataFrame."""
    logger.info("Reordering columns")
    columns_order = [
        "Vintage",
        "LotNo",
        "WineName",
        "FullWineNameWithProducer",
        "auction_unit_price",
        "min_price",
        "discount_percentage",
        "Low",
        "High",
        "Quantity",
        "BottleName",
        "Producer",
        "RegionDescription",
        "WineType",
        "query",
        "average_price",
        "description",
        "url",
        "region",
        "origin",
        "grape_variety",
        "image",
        "region_image",
        "offers_count",
    ]
    return df[columns_order]


async def analyze_auction_catalog(
    catalog_file_path: str, batch_size: int = 100
) -> pd.DataFrame:
    """Perform the full auction catalog analysis."""
    logger.info("Starting auction catalog analysis")
    catalog_df = load_catalog_data(catalog_file_path)
    catalog_df = extract_wine_names(catalog_df)
    unique_wine_names = catalog_df["FullWineNameWithProducer"].unique().tolist()

    logger.info(f"Found {len(unique_wine_names)} unique wines")

    # Create output file path based on catalog file name
    catalog_name = os.path.splitext(os.path.basename(catalog_file_path))[0]
    output_file_path = f"{catalog_name}_wine_list.csv"

    # Create a temporary file to store wine names
    temp_wine_list_file = f"{catalog_name}_temp_wine_list.txt"
    with open(temp_wine_list_file, "w") as f:
        for wine in unique_wine_names:
            f.write(f"{wine}\n")

    logger.info(f"Processing wine list with batch size {batch_size}")
    await process_wine_list(temp_wine_list_file, output_file_path, batch_size, True)

    logger.info("Loading processed wine data")
    search_results_df = pd.read_csv(output_file_path)

    processed_df = process_catalog_data(catalog_df, search_results_df)
    final_df = reorder_columns(processed_df)

    logger.info(f"Analysis complete. Processed {len(final_df)} wines.")

    # Clean up temporary file
    os.remove(temp_wine_list_file)

    return final_df


# Main execution
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    logger.add("auction_analysis.log", rotation="10 MB")

    catalog_file_path = "data/Catalog_241W_35.xlsx"

    result_df = asyncio.run(analyze_auction_catalog(catalog_file_path))

    # Create final output file path
    catalog_name = os.path.splitext(os.path.basename(catalog_file_path))[0]
    final_output_file_path = f"{catalog_name}_final_processed_wine_data.csv"

    result_df.to_csv(final_output_file_path, index=False)
    logger.info(f"Results saved to {final_output_file_path}")

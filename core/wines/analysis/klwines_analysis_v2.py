import argparse
import asyncio
import os
import re
import shutil
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from core.wines.analysis.utils import process_wine_list

DATA_DIR = "data"


def step_1_normalize_auction_lot(input_file_path: str, output_file_path: str):
    def extract_wine_info(row):
        lot_name = row["Lot Name and link to bid"]

        # Extract bottle size
        bottle_size_match = re.search(r"\((\d+(?:\.\d+)?[Ll])\)$", lot_name)
        bottle_size = bottle_size_match.group(1) if bottle_size_match else "750ml"

        # Remove bottle size from wine name if present
        wine_name = re.sub(r"\s*\(\d+(?:\.\d+)?[Ll]\)$", "", lot_name).strip()

        return pd.Series(
            {
                "Wine Name": wine_name,
                "Bottle Size": bottle_size,
                "Auction Closes": row["Auction Closes"],
                "Reserve": row["Reserve"],
                "Quantity": row["Quantity"],
            }
        )

    # Load the auction catalog file
    try:
        df = pd.read_csv(input_file_path)
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return

    # Extract wine info
    df_wine_info = df.apply(extract_wine_info, axis=1)

    # Save the cleaned data to a CSV file
    df_wine_info.to_csv(output_file_path, index=False)
    logger.info(f"Normalized auction lot data saved to {output_file_path}")


def step_2_merge_and_analyze_wine_data(
    auction_data_path: str, search_wine_path: str, output_path: str
):
    logger.info("Starting merge and analysis of wine data")
    logger.info(f"Auction data path: {auction_data_path}")
    logger.info(f"Search wine path: {search_wine_path}")
    logger.info(f"Output path: {output_path}")
    # Load data
    try:
        auction_data = pd.read_csv(auction_data_path)
    except Exception as e:
        logger.error(f"Error reading auction data: {e}")
        return

    klwine_data = pd.read_csv(search_wine_path)
    merged_data = pd.merge(
        auction_data, klwine_data, left_on="Wine Name", right_on="query", how="left"
    )

    # Normalize bottle size to handle different formats
    def normalize_bottle_size(bottle_size):
        size_str = str(bottle_size).lower()
        if "750ml" in size_str:
            return 1
        elif "1l" in size_str:
            return 1000 / 750
        elif "1.5l" in size_str:
            return 1500 / 750
        elif "3l" in size_str:
            return 3000 / 750
        elif "6l" in size_str:
            return 6000 / 750
        else:
            match = re.search(r"(\d+(?:\.\d+)?)\s*l", size_str)
            if match:
                liters = float(match.group(1))
                return (liters * 1000) / 750
            else:
                logger.warning(
                    f"Unrecognized bottle size: {bottle_size}. Defaulting to 750ml."
                )
                return 1

    merged_data["format_ratio"] = merged_data["Bottle Size"].apply(
        normalize_bottle_size
    )

    # Calculate 'bid_on_hand' as 1.1 * Reserve
    merged_data["bid_on_hand"] = merged_data["Reserve"] * 1.1

    # Compute the unit price
    merged_data["unit_price"] = (
        merged_data["bid_on_hand"]
        / merged_data["Quantity"]
        / merged_data["format_ratio"]
    )

    # Handle cases where min_price is missing by filling with unit_price
    merged_data["min_price"] = merged_data["min_price"].fillna(
        merged_data["unit_price"]
    )

    # Compute the discount as (min_price - unit_price) / min_price
    merged_data["discount"] = (
        merged_data["min_price"] - merged_data["unit_price"]
    ) / merged_data["min_price"]

    # Sort the data by discount in descending order
    sorted_data = merged_data.sort_values(by="discount", ascending=False)

    # Select relevant columns to write to the new file, keeping all columns
    final_columns = [
        "Wine Name",
        "Reserve",
        "Quantity",
        "Bottle Size",
        "bid_on_hand",
        "unit_price",
        "min_price",
        "discount",
        "url",
        "Auction Closes",
    ]
    all_columns = final_columns + [
        col for col in merged_data.columns if col not in final_columns
    ]
    final_data = sorted_data[all_columns]

    # Write the result to a new CSV file
    final_data.to_csv(output_path, index=False)
    logger.info(f"Merged and analyzed wine data saved to {output_path}")


async def analyze_auction_catalog(
    catalog_file_path: str, batch_size: int = 100
) -> pd.DataFrame:
    """Perform the full auction catalog analysis."""
    logger.info("Starting auction catalog analysis")

    # Create a new directory for this analysis run
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"klwines_{today}")
    os.makedirs(analysis_dir, exist_ok=True)

    # Copy the source file to the new directory
    source_file_name = os.path.basename(catalog_file_path)
    new_catalog_file_path = os.path.join(analysis_dir, source_file_name)
    shutil.copy2(catalog_file_path, new_catalog_file_path)

    # Step 1: Normalize auction lot
    normalized_auction_file = os.path.join(analysis_dir, "normalized_auction_lot.csv")
    step_1_normalize_auction_lot(new_catalog_file_path, normalized_auction_file)

    # Step 2: Process wine list
    wine_list_output_file = os.path.join(analysis_dir, "wine_list.csv")

    logger.info(f"Processing wine list with batch size {batch_size}")
    _ = await process_wine_list(
        normalized_auction_file, "Wine Name", wine_list_output_file, batch_size
    )

    # Step 3: Merge and analyze wine data
    final_output_file = os.path.join(analysis_dir, "final_processed_wine_data.csv")
    step_2_merge_and_analyze_wine_data(
        normalized_auction_file, wine_list_output_file, final_output_file
    )

    logger.info(f"Analysis complete. Results saved to {final_output_file}")

    return pd.read_csv(final_output_file)


if __name__ == "__main__":
    import argparse

    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description="Analyze K&L Wines auction catalog")
    parser.add_argument("file_path", help="Path to the auction catalog file")
    args = parser.parse_args()

    # Create a log file in the analysis directory
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"klwines_{today}")
    os.makedirs(analysis_dir, exist_ok=True)
    logger.add(
        os.path.join(analysis_dir, "klwines_auction_analysis.log"), rotation="10 MB"
    )

    catalog_file_path = args.file_path

    result_df = asyncio.run(analyze_auction_catalog(catalog_file_path))

    logger.info(f"Analysis completed. Processed {len(result_df)} wines.")

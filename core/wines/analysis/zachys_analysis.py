import argparse
import asyncio
import os
import re
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from core.wines.analysis.utils import process_wine_list

# Constants
DATA_DIR = "data"


def step_1_normalize_auction_lot(input_file_path: str, output_file_path: str):
    """Load the catalog data and extract wine names from Lot Title."""
    logger.info(f"Loading catalog data from {input_file_path}")
    catalog_df = pd.read_excel(input_file_path, skiprows=2)

    logger.info("Extracting wine names from Lot Title")
    catalog_df["FullWineNameWithProducer"] = catalog_df["Lot Title"].str.strip()

    # Save the normalized data
    catalog_df.to_csv(output_file_path, index=False)
    logger.info(f"Normalized auction lot data saved to {output_file_path}")


def step_2_merge_and_analyze_wine_data(
    auction_data_path: str, search_wine_path: str, output_path: str
):
    """Process catalog data and merge with search results."""
    logger.info("Starting merge and analysis of wine data")

    # Load data
    catalog_df = pd.read_csv(auction_data_path)
    search_results_df = pd.read_csv(search_wine_path)

    joined_df = catalog_df.merge(
        search_results_df,
        left_on="FullWineNameWithProducer",
        right_on="query",
        how="inner",
    )

    def normalize_size(size: str) -> float:
        """Convert size to ml."""
        size = size.lower().strip()
        if "ml" in size:
            return float(size.replace("ml", ""))
        elif "l" in size:
            return float(size.replace("l", "")) * 1000
        else:
            # Default to 750ml if size is not recognized
            logger.warning(f"Unrecognized size: {size}. Defaulting to 750ml.")
            return 750.0

    def calculate_unit_price(row: pd.Series) -> float:
        if row["Qty"] <= 0:
            return row["Low Estimate"]

        size_ml = normalize_size(row["Size"])
        size_ratio = size_ml / 750.0  # Normalize to 750ml bottle

        # Remove currency symbol and commas from Low Estimate
        low_estimate = float(re.sub(r"[^\d.]", "", str(row["Low Estimate"])))

        return (low_estimate / row["Qty"]) / size_ratio

    joined_df["auction_unit_price"] = joined_df.apply(calculate_unit_price, axis=1)
    joined_df["auction_on_hand_unit_price"] = joined_df["auction_unit_price"] * 1.25 + 7
    joined_df["discount_percentage"] = (
        (joined_df["min_price"] - joined_df["auction_on_hand_unit_price"])
        / joined_df["min_price"]
    ) * 100

    # Reorder columns
    columns_order = [
        "FullWineNameWithProducer",
        "auction_unit_price",
        "auction_on_hand_unit_price",
        "discount_percentage",
        "min_price",
        "average_price",
        "Low Estimate",
        "High Estimate",
        "Qty",
        "offers_count",
        "url",
        "Lot",
        "Size",
        "Vintage",
        "Lot Title",
        "Lot Details",
        "Producer",
        "Country",
        "Region",
        "Class",
        "OWC_OC",
        "Your Bid",
        "URL",
        "description",
        "region",
        "origin",
        "grape_variety",
        "image",
        "region_image",
        "query",
    ]

    final_df = joined_df[columns_order].sort_values(
        by="discount_percentage", ascending=False
    )

    # Save the final data
    final_df.to_csv(output_path, index=False)
    logger.info(f"Merged and analyzed wine data saved to {output_path}")


async def analyze_auction_catalog(
    catalog_file_path: str, batch_size: int = 100
) -> pd.DataFrame:
    """Perform the full auction catalog analysis for Zachys."""
    logger.info("Starting Zachys auction catalog analysis")

    # Create a new directory for this analysis run
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"zachys_{today}")
    os.makedirs(analysis_dir, exist_ok=True)

    # Step 1: Normalize auction lot
    normalized_auction_file = os.path.join(analysis_dir, "normalized_auction_lot.csv")
    step_1_normalize_auction_lot(catalog_file_path, normalized_auction_file)

    # Step 2: Process wine list
    wine_list_output_file = os.path.join(analysis_dir, "wine_list.csv")

    logger.info(f"Processing wine list with batch size {batch_size}")
    await process_wine_list(
        normalized_auction_file,
        "FullWineNameWithProducer",
        wine_list_output_file,
        batch_size,
    )

    # Step 3: Merge and analyze wine data
    final_output_file = os.path.join(analysis_dir, "final_processed_wine_data.csv")
    step_2_merge_and_analyze_wine_data(
        normalized_auction_file, wine_list_output_file, final_output_file
    )

    logger.info(f"Analysis complete. Results saved to {final_output_file}")

    return pd.read_csv(final_output_file)


if __name__ == "__main__":
    load_dotenv()

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Analyze Zachys auction catalog")
    parser.add_argument("input_file", help="Path to the input Zachys catalog file")
    args = parser.parse_args()

    # Create a log file in the analysis directory
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"zachys_{today}")
    os.makedirs(analysis_dir, exist_ok=True)
    logger.add(
        os.path.join(analysis_dir, "zachys_auction_analysis.log"), rotation="10 MB"
    )

    catalog_file_path = args.input_file

    if not os.path.exists(catalog_file_path):
        logger.error(f"Input file not found: {catalog_file_path}")
        exit(1)

    logger.info(f"Starting analysis of catalog file: {catalog_file_path}")

    result_df = asyncio.run(analyze_auction_catalog(catalog_file_path))

    logger.info(f"Analysis completed. Processed {len(result_df)} wines.")

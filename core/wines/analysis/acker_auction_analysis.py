import asyncio
import os
import shutil
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from core.wines.analysis.utils import process_wine_list

# Constants
DATA_DIR = "data"


def step_1_normalize_auction_lot(input_file_path: str, output_file_path: str):
    """Load the catalog data and extract wine names."""
    logger.info(f"Loading catalog data from {input_file_path}")
    catalog_df = pd.read_excel(input_file_path, sheet_name="qryCatalogExcel")

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

    format_multipliers = {
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

    def calculate_unit_price(row: pd.Series) -> float:
        format_multiplier = format_multipliers.get(row["BottleName"].lower(), 1)
        total_units = row["Quantity"] * format_multiplier
        return row["Low"] / total_units if total_units > 0 else row["Low"]

    joined_df["auction_unit_price"] = joined_df.apply(calculate_unit_price, axis=1)
    joined_df["auction_on_hand_unit_price"] = joined_df["auction_unit_price"] * 1.27 + 7
    joined_df["discount_percentage"] = (
        (joined_df["min_price"] - joined_df["auction_on_hand_unit_price"])
        / joined_df["min_price"]
    ) * 100

    # Reorder columns
    catalog_columns = [
        "Vintage",
        "LotNo",
        "WineName",
        "FullWineNameWithProducer",
        "Low",
        "High",
        "Quantity",
        "BottleName",
        "Producer",
        "RegionDescription",
        "WineType",
    ]
    search_columns = [
        "query",
        "average_price",
        "min_price",
        "description",
        "url",
        "region",
        "origin",
        "grape_variety",
        "image",
        "region_image",
        "offers_count",
    ]
    calculated_columns = [
        "auction_unit_price",
        "auction_on_hand_unit_price",
        "discount_percentage",
    ]
    columns_order = catalog_columns + search_columns + calculated_columns

    final_df = joined_df[columns_order].sort_values(
        by="discount_percentage", ascending=False
    )

    # Save the final data
    final_df.to_csv(output_path, index=False)
    logger.info(f"Merged and analyzed wine data saved to {output_path}")


async def analyze_auction_catalog(
    catalog_file_path: str, batch_size: int = 100
) -> pd.DataFrame:
    """Perform the full auction catalog analysis."""
    logger.info("Starting auction catalog analysis")

    # Create a new directory for this analysis run
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"acker_{today}")
    os.makedirs(analysis_dir, exist_ok=True)

    # Copy the source file to the new directory
    source_file_name = os.path.basename(catalog_file_path)
    new_catalog_file_path = os.path.join(analysis_dir, source_file_name)
    shutil.copy2(catalog_file_path, new_catalog_file_path)

    # Step 1: Normalize auction lot
    normalized_auction_file = os.path.join(analysis_dir, "normalized_auction_lot.csv")
    step_1_normalize_auction_lot(new_catalog_file_path, normalized_auction_file)

    # Step 2: Process wine list
    catalog_name = os.path.splitext(source_file_name)[0]
    wine_list_output_file = os.path.join(analysis_dir, f"{catalog_name}_wine_list.csv")

    logger.info(f"Processing wine list with batch size {batch_size}")
    await process_wine_list(
        normalized_auction_file,
        "FullWineNameWithProducer",
        wine_list_output_file,
        batch_size,
    )

    # Step 3: Merge and analyze wine data
    final_output_file = os.path.join(
        analysis_dir, f"{catalog_name}_final_processed_wine_data.csv"
    )
    step_2_merge_and_analyze_wine_data(
        normalized_auction_file, wine_list_output_file, final_output_file
    )

    logger.info(f"Analysis complete. Results saved to {final_output_file}")

    return pd.read_csv(final_output_file)


if __name__ == "__main__":
    load_dotenv()

    # Create a log file in the analysis directory
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"acker_{today}")
    os.makedirs(analysis_dir, exist_ok=True)
    logger.add(
        os.path.join(analysis_dir, "acker_auction_analysis.log"), rotation="10 MB"
    )

    catalog_file_path = os.path.join(DATA_DIR, "Catalog_241W_36.xlsx")

    result_df = asyncio.run(analyze_auction_catalog(catalog_file_path))

    logger.info(f"Analysis completed. Processed {len(result_df)} wines.")

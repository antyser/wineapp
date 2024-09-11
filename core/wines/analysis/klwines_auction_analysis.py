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
    def extract_quantity_from_title(title):
        if isinstance(title, str):
            qty_match = re.search(r"\(qty\s*:\s*(\d+)\)", title, flags=re.IGNORECASE)
            return int(qty_match.group(1)) if qty_match else 1
        return 1

    # Function to extract wine information from lot data
    def extract_wine_info(lot_data):
        wine_info = []
        for lot in lot_data:
            wine_name = lot.get("Title", "").split("(")[0].strip()
            quantity = extract_quantity_from_title(lot.get("Title", ""))
            format = "750ml"  # Default format

            # Extract format
            format_match = re.search(r"\(([\d.]+L)\)", lot.get("Title", ""))
            if format_match:
                format = format_match.group(1)

            # Extract bid
            if "Current Bid" in lot:
                bid = (
                    lot["Current Bid"]
                    .replace("Current Bid: ", "")
                    .replace("$", "")
                    .replace(",", "")
                )
            elif "Starting Bid" in lot:
                bid = (
                    lot["Starting Bid"]
                    .replace("Starting Bid: ", "")
                    .replace("$", "")
                    .replace(",", "")
                )
            else:
                bid = None

            # Extract end date and convert to ISO format
            end_date_match = re.search(r"End Date: (.+)", lot.get("Lot Details", ""))
            end_date = (
                pd.to_datetime(end_date_match.group(1)).isoformat()
                if end_date_match
                else None
            )

            # Append the extracted data
            wine_info.append(
                {
                    "Wine Name": wine_name,
                    "Quantity": quantity,
                    "Format": format,
                    "Bid": float(bid) if bid else None,
                    "End Date": end_date,
                }
            )

        return pd.DataFrame(wine_info)

    # Function to calculate unit price based on format and quantity
    def calculate_unit_price(row):
        format_size = 750.0  # Default format size is 750ml
        if row["Format"] == "1L":
            format_size = 1000.0
        elif row["Format"] == "1.5L":
            format_size = 1500.0
        elif "L" in row["Format"]:
            format_size = (
                float(row["Format"].replace("L", "")) * 1000.0
            )  # Convert liters to ml

        return (
            row["Bid"] / (row["Quantity"] * (format_size / 750.0))
            if row["Quantity"] and row["Bid"]
            else None
        )

    # Load the auction catalog file
    try:
        df = pd.read_excel(input_file_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        logger.info("Attempting to read as CSV...")
        df = pd.read_csv(input_file_path)

    # Process the catalog to extract lot data
    lot_data = []
    current_lot = {}

    for _, row in df.iterrows():
        if isinstance(row[0], str) and (
            "Current Bid" in row[0] or "Starting Bid" in row[0]
        ):
            if "Current Bid" in row[0]:
                current_lot["Current Bid"] = row[0]
            else:
                current_lot["Starting Bid"] = row[0]
            lot_data.append(current_lot)
            current_lot = {}
        elif isinstance(row[0], str) and "Bid on this" in row[0]:
            current_lot["Description"] = row[0]
        elif isinstance(row[0], str) and "This lot contains" in row[0]:
            current_lot["Lot Details"] = row[0]
        elif pd.notna(row[0]):
            current_lot["Title"] = row[0]

    # Extract wine info and calculate unit price
    df_wine_info = extract_wine_info(lot_data)
    df_wine_info["Unit Price"] = df_wine_info.apply(calculate_unit_price, axis=1)

    # Save the cleaned data to a CSV file
    df_wine_info.to_csv(output_file_path, index=False)


def step_2_merge_and_analyze_wine_data(
    auction_data_path: str, search_wine_path: str, output_path: str
):
    logger.info("Starting merge and analysis of wine data")

    # Load data
    try:
        auction_data = pd.read_excel(auction_data_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Error reading auction data: {e}")
        logger.info("Attempting to read as CSV...")
        auction_data = pd.read_csv(auction_data_path)

    klwine_data = pd.read_csv(search_wine_path)
    merged_data = pd.merge(
        auction_data, klwine_data, left_on="Wine Name", right_on="query", how="left"
    )

    # Calculate 'bid_on_hand' as 1.1 * Bid
    merged_data["bid_on_hand"] = merged_data["Bid"] * 1.1

    # Normalize format to handle different bottle sizes
    def normalize_format(row):
        format_str = row["Format"].lower()
        if "750ml" in format_str:
            return 1
        elif "1l" in format_str:
            return 1000 / 750
        elif "1.5l" in format_str:
            return 1500 / 750
        elif "3l" in format_str:
            return 3000 / 750
        elif "6l" in format_str:
            return 6000 / 750
        else:
            match = re.search(r"(\d+(?:\.\d+)?)\s*l", format_str)
            if match:
                liters = float(match.group(1))
                return (liters * 1000) / 750
            else:
                logger.warning(
                    f"Unrecognized format: {row['Format']}. Defaulting to 750ml."
                )
                return 1

    merged_data["format_ratio"] = merged_data.apply(normalize_format, axis=1)

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
        "Bid",
        "Quantity",
        "Format",
        "bid_on_hand",
        "unit_price",
        "min_price",
        "discount",
        "url",
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
    catalog_name = os.path.splitext(source_file_name)[0]
    wine_list_output_file = os.path.join(analysis_dir, f"{catalog_name}_wine_list.csv")

    logger.info(f"Processing wine list with batch size {batch_size}")
    _ = await process_wine_list(
        normalized_auction_file, "Wine Name", wine_list_output_file, batch_size
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

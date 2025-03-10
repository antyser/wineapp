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

# Constants
DATA_DIR = "data"

# Ensure the data directory exists
os.makedirs(DATA_DIR, exist_ok=True)


# Define standard columns for downstream processing
STANDARD_COLUMNS = [
    "wine_name",
    "quantity",
    "format",
    "auction_price",
]


# Normalization functions for each auction house
def normalize_auction_data_acker(catalog_file_path: str) -> pd.DataFrame:
    """Normalize Acker auction data."""
    # Load the catalog data
    logger.info(f"Loading Acker catalog data from {catalog_file_path}")
    catalog_df = pd.read_excel(catalog_file_path, sheet_name="qryCatalogExcel")

    logger.info("Extracting wine names and normalizing data")

    # Combine wine name components
    def combine_wine_name(row):
        parts = [
            str(row["Vintage"]) if pd.notnull(row["Vintage"]) else "",
            row["Producer"] if pd.notnull(row["Producer"]) else "",
            row["WineName"].strip() if pd.notnull(row["WineName"]) else "",
            row["Designation"] if pd.notnull(row["Designation"]) else "",
        ]
        return " ".join(part for part in parts if part)

    catalog_df["wine_name"] = catalog_df.apply(combine_wine_name, axis=1)

    # Extract quantity
    catalog_df["quantity"] = catalog_df["Quantity"].fillna(1).astype(int)

    # Map formats to standard format
    format_mapping = {
        "bottle": "750ml",
        "magnum": "1.5l",
        "half-bottle": "375ml",
        "jeroboam": "3l",
        "double magnum": "3l",
        "methuselah": "6l",
        "nebuchadnezzar": "15l",
        "imperial": "6l",
        "6 liter": "6l",
    }
    catalog_df["format"] = catalog_df["BottleName"].fillna("Bottle").str.lower()
    catalog_df["format"] = catalog_df["format"].replace(format_mapping)

    # Auction price is in "Low"
    catalog_df["auction_price"] = catalog_df["Low"].astype(float)

    # Return DataFrame with standard columns
    return catalog_df[
        STANDARD_COLUMNS
        + [col for col in catalog_df.columns if col not in STANDARD_COLUMNS]
    ]


def normalize_auction_data_zachys(catalog_file_path: str) -> pd.DataFrame:
    """Normalize Zachys auction data."""
    # Load the catalog data
    logger.info(f"Loading Zachys catalog data from {catalog_file_path}")
    catalog_df = pd.read_excel(catalog_file_path, skiprows=2)

    logger.info("Extracting wine names and normalizing data")

    # Wine name is in "Lot Title"
    catalog_df["wine_name"] = catalog_df["Lot Title"].str.strip()

    # Quantity is in "Qty"
    catalog_df["quantity"] = catalog_df["Qty"].fillna(1).astype(int)

    # Format is in "Size"
    size_mapping = {
        "Half Bottle": "375ml",
        "Bottle": "750ml",
        "Magnum": "1.5l",
        # Add other mappings as needed
    }
    catalog_df["format"] = catalog_df["Size"].fillna("750ml").str.title()
    catalog_df["format"] = catalog_df["format"].replace(size_mapping)

    # Auction price is in "Low Estimate"
    catalog_df["auction_price"] = catalog_df["Low Estimate"].astype(float)

    # Return DataFrame with standard columns
    return catalog_df[
        STANDARD_COLUMNS
        + [col for col in catalog_df.columns if col not in STANDARD_COLUMNS]
    ]


def normalize_auction_data_klwines(catalog_file_path: str) -> pd.DataFrame:
    """Normalize K&L Wines auction data assuming quantity is always 1."""
    # Load the auction catalog file
    logger.info(f"Loading K&L Wines catalog data from {catalog_file_path}")
    try:
        df = pd.read_excel(catalog_file_path)
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        logger.info("Attempting to read as CSV...")
        df = pd.read_csv(catalog_file_path)

    # Map the unit sizes to a standardized format (ml)
    def unit_size_to_standard_format(unit_size):
        unit_size = str(unit_size).lower().strip()
        if "ml" in unit_size:
            return unit_size
        elif "liter" in unit_size:
            return f"{float(unit_size.replace('liter', '').strip()) * 1000}ml"
        else:
            return "750ml"  # default if unknown

    def clean_and_combine_wine_name(row):
        # Handle NaN/float values in the name column
        if pd.isna(row["name"]):
            return ""

        # Convert to string and then clean
        name = str(row["name"]).strip()
        clean_name = re.sub(r"\s*\([^)]*[Ll]\)\s*$", "", name)

        # Combine vintage and clean wine name
        return f"{row['vintage']} {clean_name}"

    # Clean wine name by removing content in parentheses at the end
    def clean_wine_name(name):
        return re.sub(r"\s*\([^)]*\)\s*$", "", str(name).strip())

    # Standardize the data, assuming all quantities are 1
    df["format"] = df["unit-size"].apply(unit_size_to_standard_format)
    df["quantity"] = 1  # Assuming all quantities are 1
    df["auction_price"] = pd.to_numeric(
        df["price"].replace("[\$,]", "", regex=True), errors="coerce"
    )
    df["wine_name"] = df.apply(clean_and_combine_wine_name, axis=1)
    df["auction_url"] = df["url"]

    # Select only the required columns
    required_columns = [
        "wine_name",
        "quantity",
        "format",
        "auction_price",
        "auction_url",
    ]
    return df[required_columns]


def normalize_auction_data_hdh(catalog_file_path: str) -> pd.DataFrame:
    """Normalize HDH auction data."""
    logger.info(f"Loading HDH catalog data from {catalog_file_path}")
    catalog_df = pd.read_excel(
        catalog_file_path, sheet_name="Auction Catalog With Scores"
    )

    logger.info("Extracting wine names and normalizing data")

    def reformat_hdh_wine_name(wine_name):
        """
        Reformat HDH wine name to match Wine-Searcher's expected format.

        From: [Vintage] [Appellation], [Vineyard], [Producer]
        To:   [Vintage] [Producer] [Vineyard] [Appellation]
        """
        # First, remove any bottle size information
        wine_name = re.sub(r"\s*\([^)]*[Ll]\)\s*$", "", str(wine_name).strip())

        # Extract vintage if present
        vintage_match = re.search(r"^(\d{4})\s+", wine_name)
        vintage = ""
        if vintage_match:
            vintage = vintage_match.group(1)
            wine_name = wine_name[len(vintage) :].strip()

        # Split the remaining parts by commas
        parts = [part.strip() for part in wine_name.split(",")]

        if len(parts) >= 3:
            # If we have at least 3 parts, assume format: [Appellation], [Vineyard], [Producer]
            appellation = parts[0]
            vineyard = parts[1]
            producer = parts[2]

            # Reconstruct in Wine-Searcher format: [Vintage] [Producer] [Vineyard] [Appellation]
            reformatted = f"{vintage} {producer} {vineyard} {appellation}".strip()
            return reformatted
        elif len(parts) == 2:
            # If we have 2 parts, assume format: [Appellation], [Producer]
            appellation = parts[0]
            producer = parts[1]

            # Reconstruct in Wine-Searcher format: [Vintage] [Producer] [Appellation]
            reformatted = f"{vintage} {producer} {appellation}".strip()
            return reformatted
        else:
            # If we only have 1 part or can't parse properly, return the original with vintage
            return f"{vintage} {parts[0]}".strip()

    catalog_df["wine_name"] = catalog_df["Wine Name"].apply(reformat_hdh_wine_name)
    catalog_df["quantity"] = catalog_df["Qty"].fillna(1).astype(int)

    def literage_to_format(literage):
        if literage == 750:
            return "750ml"
        elif literage == 1500:
            return "1.5L"
        elif literage == 3000:
            return "3L"
        else:
            return f"{literage}ml"

    catalog_df["format"] = catalog_df["Literage"].apply(literage_to_format)
    catalog_df["auction_price"] = catalog_df["Low Est"].astype(float)

    # Use only the columns present in the DataFrame
    available_columns = [col for col in STANDARD_COLUMNS if col in catalog_df.columns]
    additional_columns = [
        col for col in catalog_df.columns if col not in STANDARD_COLUMNS
    ]

    return catalog_df[available_columns + additional_columns]


def normalize_auction_data_spectrum(catalog_file_path: str) -> pd.DataFrame:
    """Normalize Spectrum auction data."""
    logger.info(f"Loading Spectrum catalog data from {catalog_file_path}")
    catalog_df = pd.read_excel(catalog_file_path)

    logger.info("Extracting wine names and normalizing data")

    # Wine name is in "LotHeading"
    catalog_df["wine_name"] = catalog_df["LotHeading"].str.strip()

    # Quantity is in "BottleQuantity"
    catalog_df["quantity"] = catalog_df["BottleQuantity"].fillna(1).astype(int)

    # Format is in "BottleFormat" - convert to milliliters
    catalog_df["format"] = (
        catalog_df["BottleFormat"]
        .fillna(0.75)
        .apply(lambda x: f"{int(float(x) * 1000)}ml")
    )

    # Auction price is in "OpeningBid"
    catalog_df["auction_price"] = catalog_df["OpeningBid"].astype(float)

    # Return DataFrame with standard columns
    return catalog_df[
        STANDARD_COLUMNS
        + [col for col in catalog_df.columns if col not in STANDARD_COLUMNS]
    ]


def normalize_auction_data(catalog_file_path: str, auction_house: str) -> pd.DataFrame:
    """Normalize auction data from the specified auction house."""
    if auction_house.lower() == "acker":
        return normalize_auction_data_acker(catalog_file_path)
    elif auction_house.lower() == "zachys":
        return normalize_auction_data_zachys(catalog_file_path)
    elif auction_house.lower() == "klwines":
        return normalize_auction_data_klwines(catalog_file_path)
    elif auction_house.lower() == "hdh":
        return normalize_auction_data_hdh(catalog_file_path)
    elif auction_house.lower() == "spectrum":
        return normalize_auction_data_spectrum(catalog_file_path)
    else:
        raise ValueError(f"Unsupported auction house: {auction_house}")


def merge_and_analyze_wine_data(
    auction_data_path: str, search_wine_path: str, auction_house: str
) -> pd.DataFrame:
    """Process catalog data and merge with search results."""
    logger.info("Starting merge and analysis of wine data")

    # Load data
    auction_df = pd.read_csv(auction_data_path)
    wine_df = pd.read_csv(search_wine_path)

    # Merge on 'wine_name' and 'query'
    joined_df = auction_df.merge(
        wine_df,
        left_on="wine_name",
        right_on="query",
        how="inner",
    )

    def format_to_ml(format_str: str) -> int:
        """
        Convert a wine format string to milliliters.

        Args:
        format_str (str): The format string to convert.

        Returns:
        int: The equivalent volume in milliliters.
        """
        format_str = str(format_str).lower().strip()

        # Handle 'ml' suffix
        if format_str.endswith("ml"):
            return int(float(format_str[:-2]))

        # Handle 'l' or 'liter' suffix

        if format_str.endswith("l") or format_str.endswith("liter"):
            try:
                if format_str == "liter":
                    return 1000
                liters = float(format_str.rstrip("literl"))
                return int(liters * 1000)
            except ValueError:
                logger.warning(f"Unknown format: {format_str}")
                return 750  # Default to standard bottle size
        # Handle specific formats
        format_mapping = {
            "half bottle": 375,
            "bottle": 750,
            "magnum": 1500,
            "double magnum": 3000,
            "jeroboam": 3000,
            "rehoboam": 4500,
            "methuselah": 6000,
            "salmanazar": 9000,
            "balthazar": 12000,
            "nebuchadnezzar": 15000,
        }

        for key, value in format_mapping.items():
            if key in format_str:
                return value

        # If it's a number without a unit, assume it's in ml
        try:
            return int(float(format_str))
        except ValueError:
            logger.warning(f"Unknown format: {format_str}")
            return 750  # Default to standard bottle size

    joined_df["format_ml"] = joined_df["format"].apply(format_to_ml)

    # Calculate unit price
    # Unit price = auction_price / (quantity * (format_ml / 750))
    joined_df["unit_price"] = joined_df.apply(
        lambda row: (
            row["auction_price"] / (row["quantity"] * (row["format_ml"] / 750.0))
            if row["quantity"] > 0
            else row["auction_price"]
        ),
        axis=1,
    )

    # Calculate 'auction_on_hand_unit_price' based on auction house
    if auction_house.lower() == "klwines":
        joined_df["auction_on_hand_unit_price"] = joined_df["unit_price"] * 1.1
    elif auction_house.lower() == "hdh":
        joined_df["auction_on_hand_unit_price"] = joined_df["unit_price"] * 1.195 + 7
    elif auction_house.lower() in ["acker", "zachys"]:
        joined_df["auction_on_hand_unit_price"] = joined_df["unit_price"] * 1.25 + 7
    elif auction_house.lower() == "spectrum":
        joined_df["auction_on_hand_unit_price"] = joined_df["unit_price"] * 1.22 + 7
    else:
        raise ValueError(f"Unsupported auction house: {auction_house}")

    # Compute 'discount_percentage'
    joined_df["discount_percentage"] = (
        (joined_df["min_price"] - joined_df["auction_on_hand_unit_price"])
        / joined_df["min_price"]
    ) * 100

    # Reorder columns, keeping important ones at the beginning
    important_columns = [
        "wine_name",
        "unit_price",
        "auction_on_hand_unit_price",
        "discount_percentage",
        "min_price",
        "average_price",
        "auction_price",
        "quantity",
        "format",
        "url",
    ]

    # Add any columns from auction_df and wine_df that are not already in the list
    all_columns = important_columns + [
        col for col in joined_df.columns if col not in important_columns
    ]

    final_df = joined_df[all_columns]

    # Sort by 'discount_percentage' descending
    final_df = final_df.sort_values(by="discount_percentage", ascending=False)

    return final_df


async def analyze_auction_catalog(
    catalog_file_path: str, auction_house: str, batch_size: int = 100
) -> pd.DataFrame:
    """Perform the full auction catalog analysis."""
    logger.info(f"Starting auction catalog analysis for {auction_house}")

    # Create a new directory for this analysis run
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"{auction_house}_{today}")
    os.makedirs(analysis_dir, exist_ok=True)

    # Copy the source file to the new directory
    source_file_name = os.path.basename(catalog_file_path)
    new_catalog_file_path = os.path.join(analysis_dir, source_file_name)
    shutil.copy2(catalog_file_path, new_catalog_file_path)

    # Step 1: Normalize auction lot
    normalized_auction_file = os.path.join(analysis_dir, "normalized_auction_lot.csv")
    normalized_df = normalize_auction_data(catalog_file_path, auction_house)
    normalized_df.to_csv(normalized_auction_file, index=False)
    logger.info(f"Normalized auction data saved to {normalized_auction_file}")

    # Step 2: Process wine list
    wine_list_output_file = os.path.join(analysis_dir, "wine_list.csv")
    await process_wine_list(
        normalized_auction_file,
        "wine_name",
        wine_list_output_file,
        batch_size,
    )

    # Step 3: Merge and analyze wine data
    final_output_file = os.path.join(analysis_dir, "final_processed_wine_data.csv")
    final_df = merge_and_analyze_wine_data(
        normalized_auction_file, wine_list_output_file, auction_house
    )
    final_df.to_csv(final_output_file, index=False)

    logger.info(f"Analysis complete. Results saved to {final_output_file}")

    return final_df


if __name__ == "__main__":
    import argparse
    import shutil

    from dotenv import load_dotenv

    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description="Analyze auction catalog")
    parser.add_argument("input_file", help="Path to the input catalog file")
    parser.add_argument("auction_house", help="Name of the auction house")
    parser.add_argument(
        "--batch_size",
        type=int,
        default=100,
        help="Batch size for processing wine list",
    )
    args = parser.parse_args()

    # Set up logging
    today = datetime.now().strftime("%Y%m%d")
    analysis_dir = os.path.join(DATA_DIR, f"{args.auction_house}_{today}")
    os.makedirs(analysis_dir, exist_ok=True)
    logger.add(
        os.path.join(analysis_dir, f"{args.auction_house}_auction_analysis.log"),
        rotation="10 MB",
    )

    catalog_file_path = args.input_file
    auction_house = args.auction_house

    if not os.path.exists(catalog_file_path):
        logger.error(f"Input file not found: {catalog_file_path}")
        exit(1)

    logger.info(
        f"Starting analysis of catalog file: {catalog_file_path} for {auction_house}"
    )

    result_df = asyncio.run(
        analyze_auction_catalog(catalog_file_path, auction_house, args.batch_size)
    )

    logger.info(f"Analysis completed. Processed {len(result_df)} wines.")

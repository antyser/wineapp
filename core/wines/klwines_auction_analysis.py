import re

import pandas as pd

# Constants
DATA_DIR = "data"


def normalize_auction_lot(input_file_path: str, output_file_path: str):
    # Function to extract quantity from the title
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
    df = pd.read_excel(input_file_path)

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


""" 
def load_catalog_data(file_path: str = "data/Auction Lots 9-2-2024.xlsx") -> pd.DataFrame:
    logger.info(f"Loading catalog data from {file_path}")
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Successfully loaded {len(df)} rows from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading catalog data from {file_path}: {e}")
        raise

def extract_wine_info(lot_data: List[Dict]) -> pd.DataFrame:
    wine_info = []
    for lot in lot_data:
        wine_name = lot['Title'].split('(')[0].strip()
        quantity = 1  # Default quantity
        format = '750ml'  # Default format
        
        # Extract quantity if available
        qty_match = re.search(r'(\d+)-bottle lot', lot.get('Description', ''))
        if qty_match:
            quantity = int(qty_match.group(1))
        
        # Extract format if available
        format_match = re.search(r'\(([\d.]+L)\)', lot['Title'])
        if format_match:
            format = format_match.group(1)
        
        # Extract bid information
        if 'Current Bid' in lot:
            bid = lot['Current Bid'].replace('Current Bid: ', '')
            bid_type = 'Current Bid'
        elif 'Starting Bid' in lot:
            bid = lot['Starting Bid'].replace('Starting Bid: ', '')
            bid_type = 'Starting Bid'
        else:
            bid = None
            bid_type = None
        
        wine_info.append({
            'Wine Name': wine_name,
            'Quantity': quantity,
            'Format': format,
            'Bid': bid,
            'Bid Type': bid_type,
            'End Date': re.search(r'End Date: (.+)', lot.get('Lot Details', '')).group(1) if 'Lot Details' in lot else None
        })
    
    return pd.DataFrame(wine_info)

def process_catalog_data(
    catalog_df: pd.DataFrame, search_results_df: pd.DataFrame
) -> pd.DataFrame:
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

    joined_df["auction_on_hand_unit_price"] = joined_df["auction_unit_price"] * 1.27 + 7
    joined_df["discount_percentage"] = (
        (joined_df["min_price"] - joined_df["auction_on_hand_unit_price"])
        / joined_df["min_price"]
    ) * 100
    return joined_df.sort_values(by="discount_percentage", ascending=False)

async def analyze_auction_catalog(file_path: str, batch_size: int = 100) -> pd.DataFrame:
    logger.info("Starting auction catalog analysis")
    
    # Load and process catalog data
    df = load_catalog_data(file_path)
    lot_data = []
    current_lot = {}
    
    for _, row in df.iterrows():
        if isinstance(row[0], str) and ('Current Bid' in row[0] or 'Starting Bid' in row[0]):
            if 'Current Bid' in row[0]:
                current_lot['Current Bid'] = row[0]
            else:
                current_lot['Starting Bid'] = row[0]
            lot_data.append(current_lot)
            current_lot = {}
        elif isinstance(row[0], str) and 'Bid on this' in row[0]:
            current_lot['Description'] = row[0]
        elif isinstance(row[0], str) and 'This lot contains' in row[0]:
            current_lot['Lot Details'] = row[0]
        elif pd.notna(row[0]):
            current_lot['Title'] = row[0]
    
    # Extract wine information
    wine_df = extract_wine_info(lot_data)
    
    # Prepare wine names for batch fetching
    wine_names = wine_df['Wine Name'].unique().tolist()
    
    # Check for existing search results
    search_results_file = os.path.join(DATA_DIR, "klwines_search_results.csv")
    if os.path.exists(search_results_file):
        existing_results = pd.read_csv(search_results_file)
        existing_wines = existing_results['query'].tolist()
        wine_names = [wine for wine in wine_names if wine not in existing_wines]
        logger.info(f"Found {len(existing_wines)} existing search results. {len(wine_names)} wines left to process.")
    else:
        existing_results = pd.DataFrame()
    
    # Batch fetch wine information
    logger.info(f"Fetching wine information for {len(wine_names)} wines")
    wine_info = await batch_fetch_wines(wine_names, batch_size)
    wine_csv = wines_to_csv(wine_info)
    
    # Reorder columns
    columns_order = [
        'Wine Name', 'Quantity', 'Format', 'Bid', 'Bid Type', 'End Date',
        'average_price', 'min_price', 'description', 'url', 'region',
        'origin', 'grape_variety', 'image', 'region_image', 'offers_count'
    ]
    result_df = result_df[columns_order]
    
    return result_df

if __name__ == "__main__":
    load_dotenv(override=True)
    logger.add(os.path.join(DATA_DIR, "klwines_auction_analysis.log"), rotation="10 MB")

    catalog_file_path = os.path.join(DATA_DIR, "Auction Lots 9-2-2024.xlsx")

    result_df = asyncio.run(analyze_auction_catalog(catalog_file_path))

    output_file_path = os.path.join(DATA_DIR, "klwines_processed_wine_data.csv")
    result_df.to_csv(output_file_path, index=False)
    logger.info(f"Results saved to {output_file_path}") """

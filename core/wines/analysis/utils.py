import csv
import os
from typing import Dict, List, Set

import pandas as pd
from loguru import logger

from core.wines.model import Wine
from core.wines.wine_searcher import batch_fetch_wines


async def process_wine_list(
    input_file: str, wine_name_field: str, output_file: str, batch_size: int = 100
) -> pd.DataFrame:
    """
    Process a list of wine names from an input file, fetch their details, store them in CSV format,
    and return the results as a DataFrame.

    Args:
    input_file (str): Path to the input file containing wine names.
    wine_name_field (str): Name of the column in input file containing wine names.
    output_file (str): Path to the output CSV file.
    batch_size (int): Number of wines to process in each batch.

    Returns:
    pd.DataFrame: DataFrame containing the processed wine data.
    """
    processed_wines: Set[str] = set()
    all_results: List[Dict] = []

    # If output file exists, read already processed wines
    if os.path.exists(output_file):
        with open(output_file, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            processed_wines = set(row["query"] for row in reader)
            all_results = list(reader)

    # Read input file
    input_df = pd.read_csv(input_file)
    wine_names = input_df[wine_name_field].unique().tolist()
    total_wines = len(wine_names)
    wines_to_process = [name for name in wine_names if name not in processed_wines]

    logger.info(f"Total wines: {total_wines}")
    logger.info(f"Wines to process: {len(wines_to_process)}")

    for i in range(0, len(wines_to_process), batch_size):
        batch = wines_to_process[i : i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}")

        try:
            results = await batch_fetch_wines(batch, True)

            # Prepare data for CSV and DataFrame
            csv_data = []
            for wine_name, wine in results.items():
                if wine:
                    wine_dict = wine.model_dump()
                    wine_dict["query"] = wine_name  # Add query field
                    offers = wine.offers[:3] if wine.offers else []
                    for j in range(3):
                        if j < len(offers):
                            wine_dict[f"offer_{j+1}"] = offers[j].model_dump()
                        else:
                            wine_dict[f"offer_{j+1}"] = None
                    csv_data.append(wine_dict)

            # Append to CSV file
            file_exists = os.path.exists(output_file)
            with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
                fieldnames = (
                    list(Wine.model_fields.keys())
                    + ["query"]
                    + [f"offer_{j+1}" for j in range(3)]
                )
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()

                for row in csv_data:
                    writer.writerow(row)

            processed_wines.update(batch)
            all_results.extend(csv_data)

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Continue with the next batch

    logger.info("Wine processing completed")

    # Convert all results to DataFrame
    result_df = pd.DataFrame(all_results)
    return result_df

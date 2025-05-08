import os
import glob
import pandas as pd


def merge_batch_files(
    input_directory="data",
    batch_pattern="final_*.csv",
    output_file="data/final_merged.csv",
):
    """
    Merges all batch CSV files from the specified directory into one final CSV file.

    Parameters:
        input_directory (str): The directory where batch files are stored.
        batch_pattern (str): The glob pattern to match batch CSV files.
        output_file (str): The path for storing the merged CSV.

    Returns:
        pd.DataFrame: The final merged DataFrame (None if no files were found).
    """

    # Use glob to find all files matching the pattern in the specified directory.
    file_pattern = os.path.join(input_directory, batch_pattern)
    batch_files = glob.glob(file_pattern)

    if not batch_files:
        print("No batch files found.")
        return None

    # Read and accumulate all DataFrames.
    dataframes = []
    for file in batch_files:
        try:
            df = pd.read_csv(file)
            dataframes.append(df)
            print(f"Read {len(df)} records from {file}")
        except Exception as e:
            print(f"Error reading file {file}: {e}")

    if not dataframes:
        print("No data could be read from the batch files.")
        return None

    # Concatenate all DataFrames.
    merged_df = pd.concat(dataframes, ignore_index=True)

    # Optionally sort the merged DataFrame.
    if "fund_name" in merged_df.columns and "filing_date" in merged_df.columns:
        merged_df["filing_date"] = pd.to_datetime(
            merged_df["filing_date"], errors="coerce"
        )
        merged_df = merged_df.sort_values(by=["fund_name", "filing_date"])

    # Write the merged DataFrame to the output CSV.
    merged_df.to_csv(output_file, index=False)
    print(
        f"Merged {len(batch_files)} files with a total of {len(merged_df)} records into {output_file}"
    )

    return merged_df

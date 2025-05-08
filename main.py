import asyncio, sys
from dotenv import load_dotenv

from src.scraper import ThirteenFScraper
from src.utils import merge_batch_files

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()


def prompt_user():
    print("Please choose one of the following options:")
    print("1) Full Scrape - Scrape all managers and holdings (A-Z)")
    print("2) Merge Batches - Merge all batch CSV files into one final CSV")
    print(
        "3) Re-Scrape Specific Batch - Scrape a specific letter batch (in case of errors)"
    )
    print("4) Exit")
    return input("Enter your choice (1-4): ").strip()


def main():
    while True:
        choice = prompt_user()
        if choice == "1":
            # scrape of all managers and holdings.
            print("Starting full scrape. This may take a while...")
            scraper = ThirteenFScraper()
            asyncio.run(scraper.run())
            print("Full scrape completed.")
        elif choice == "2":
            # merge all batch files into one final CSV.
            print("Merging batch files into one final CSV...")
            merged_df = merge_batch_files(input_directory="data/batches")
            if merged_df is not None:
                print("Merge completed successfully.")
            else:
                print("No batch files were found to merge.")
        elif choice == "3":
            # re-scrape a specific batch based on the initial letter.
            letter = (
                input(
                    "Enter the letter for which you wish to re-scrape managers (A-Z): "
                )
                .strip()
                .upper()
            )
            if not letter or len(letter) != 1 or not letter.isalpha():
                print("Invalid input. Please enter a single letter (A-Z).")
                continue

            print(f"Starting batch scrape for managers starting with '{letter}'")
            scraper = ThirteenFScraper()
            asyncio.run(scraper.run_batch(letter))
            print(f"Batch scrape for letter '{letter}' completed.")
        elif choice == "4":
            print("Exiting. Goodbye!")
            sys.exit(0)
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()

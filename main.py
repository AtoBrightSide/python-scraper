import asyncio
from src.scraper import ThirteenFScraper


def main():
    output_filename = input("Enter output file name: ")
    scraper = ThirteenFScraper(output_filename=f"{output_filename}.csv")
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()

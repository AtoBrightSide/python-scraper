from src.scraper import ThirteenFScraper


def main():
    scraper = ThirteenFScraper()
    scraper.run(output_filename='./data/final.csv')


if __name__ == "__main__":
    main()

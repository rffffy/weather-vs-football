import logging

from data_extractors.weather import weather_extractor
from data_extractors.premier_league import pl_extractor

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
    logger.info("---Extracting Premier League info---")
    pl_extractor.extract()
    logger.info("---Extracting weather info---")
    weather_extractor.extract()

if __name__ == "__main__":
    main()

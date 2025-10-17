####
## dibimbing.id - Case Study ETL
## Mario Caesar // linkedin.com/in/caesarmario
## -- Python file for logging purposes
####

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("extract_open_meteo_to_minio")
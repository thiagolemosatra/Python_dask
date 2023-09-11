from loguru import logger
import sys
from datetime import datetime



timestamp=datetime.today().strftime('%Y-%m-%d_%H:%M:%S')
logger.remove(0) # remove the default handler configuration
logger.add(f'out_{timestamp}.log', level="TRACE", serialize=True)
logger.trace("Executing program")
logger.debug("Processing data...")
logger.info("Server started successfully.")
logger.success("Data processing completed successfully.")
logger.warning("Invalid configuration detected.")
logger.error("Failed to connect to the database.")
logger.critical("Unexpected system error occurred. Shutting down.")


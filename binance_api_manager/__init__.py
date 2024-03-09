import os
import sys

# Add the path to the root directory of the project

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

# Import the modules

from MarketDataFetcher import MarketDataFetcher
from OldDataFetcher import OldTradesFetcher

# Path: binance_api_manager/__init__.py
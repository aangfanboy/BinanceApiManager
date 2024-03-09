import unittest
import time

from binance_api_manager import MarketDataFetcher
from binance_api_manager import OldTradesFetcher

class DataFetchersTest(unittest.TestCase):
    def test_market_data_fetcher(self):
        fetcher = MarketDataFetcher(type='spot', symbol='ETHUSDT')
        fetcher.start()

        time.sleep(3)

        messages = fetcher.get_messages(clear=True)
        self.assertTrue(len(messages) > 0)
        self.assertTrue(isinstance(messages[0], str))
        self.assertFalse(len(fetcher.messages) > 0)

        print(f"\nSample message: {messages[0]}")

        fetcher.stop()
        self.assertFalse(fetcher.running)

    def test_old_data_fetcher(self):
        fetcher = OldTradesFetcher(type='spot', symbol='btcusdt')
        # fetch 3 minutes of data 
        fetcher.start(1646832042000, 1646832222000)

        messages = fetcher.get_messages(clear=True)
        self.assertTrue(len(messages) > 0)
        self.assertTrue(isinstance(messages[0], list))
        self.assertTrue(isinstance(messages[0][0], int))
        self.assertTrue(isinstance(messages[0][1], float))
        self.assertTrue(isinstance(messages[0][2], float))
        self.assertFalse(len(fetcher.messages) > 0)

        print(f"\nSample message: {messages[0]}")


# Path: tests/DataFetchersTest.py
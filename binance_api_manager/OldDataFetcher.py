import requests
import logging

import time

class OldTradesFetcher:
    def __init__(self, type = 'futures', symbol = 'btcusdt', keep_milliseconds = True):
        self.ws = None
        self.type = type
        self.symbol = symbol.lower() if type == 'futures' else symbol.upper()
        self.fetching = False
        self.modifing = False
        self.divider = 1000 if keep_milliseconds else 1

        self.messages = []

    def start(self, from_unix_time, until_unix_time):
        assert from_unix_time < until_unix_time
        assert len(f"{from_unix_time}") == 13
        assert len(f"{until_unix_time}") == 13

        start_time = time.time()

        if self.type == 'futures':
            url = f"https://fapi.binance.com/fapi/v1/aggTrades?"
        else:
            url = f"https://api.binance.com/api/v3/aggTrades?"

        logging.info(f"Fetching trades between {from_unix_time} and {until_unix_time}")

        from_id = None
        self.fetching = True

        while True:
            if from_id:
                params = {
                    'symbol': self.symbol,
                    'fromId': from_id,
                    'limit': 1000
                }
            else:
                params = {
                    'symbol': self.symbol,
                    'startTime': from_unix_time,
                    'limit': 1000
                }

            response = requests.get(url, params=params)

            if response.status_code != 200:
                logging.error(f"Error fetching trades: {response.text}")
                
                break

            trades = response.json()

            if len(trades) < 1000:
                break

            if trades[-1]['T'] > until_unix_time:
                while trades[-1]['T'] > until_unix_time:
                    trades.pop()

                break

            [self.messages.append(
                [
                    int(message['T']/self.divider),
                    float(message['p']),
                    float(message['q']),
                    int(message['m']),
                ]
            ) for message in trades]

            from_id = trades[-1]['a']
            last_unix_time = trades[-1]['T']

            logging.info(f"remaining: {(until_unix_time - last_unix_time) / 1000 / 60 / 60} hours of trade, completed: {round(((last_unix_time - from_unix_time) / (until_unix_time - from_unix_time)) * 100, 2)}%, total: {len(self.messages)}")
            
        self.fetching = False

        while self.modifing:
            time.sleep(0.1)

        logging.info(f"Trades fetched between {from_unix_time} and {until_unix_time}\ntotal: {len(self.messages)} trades took place in {(until_unix_time - from_unix_time) / 1000 / 60 / 60} hours\nTraced in {time.time() - start_time} seconds --- {len(self.messages) / (time.time() - start_time)} trades traced per second")
        logging.info(f"Bytes: {len(str(self.messages))/1024/1024} MB")

    def get_messages(self, clear = False):
        if clear:
            messages = self.messages
            self.messages = []
            return messages

        return self.messages   
    

# Path: binance_api_manager/OldDataFetcher.py

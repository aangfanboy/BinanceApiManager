import threading
import websocket
import logging  
import time

import json

class MarketDataFetcher:
    def __init__(self, type = 'futures', symbol = 'btcusdt', process = "trade", keep_milliseconds = True):
        self.ws = None
        self.thread = None
        self.running = False
        self.wait_req = False
        self.divider = 1000 if keep_milliseconds else 1
        self.type = type
        self.process = process
        self.symbol = symbol.lower()

        self.messages = []

    def start(self):
        if self.type == 'futures':
            self.ws = websocket.WebSocketApp(f"wss://fstream.binance.com/ws/{self.symbol}@{self.process}", on_message=self.on_message, on_error=self.on_error, on_close=self.stop)
        else:
            self.ws = websocket.WebSocketApp(f"wss://stream.binance.com/ws/{self.symbol}@{self.process}", on_message=self.on_message, on_error=self.on_error, on_close=self.stop)

        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()
        self.running = True

    def on_error(self, ws, error):
        logging.error(error)

    def on_message(self, ws, message):
        while self.wait_req:
            time.sleep(0.1)

        if self.process == "trade":
            message = json.loads(message)
            message = [
                int(message['T']/self.divider),
                float(message['p']),
                float(message['q']),
                int(message['m']),
            ]

        self.messages.append(message)

    def stop(self, *_):
        logging.info("### closed ###")
        self.ws.close()
        self.running = False

    def yield_messages(self):
        while self.running:
            for message in self.messages:
                yield message

    def get_messages(self, clear = False):
        if clear:
            self.wait_req = True

            messages = self.messages
            self.messages = []

            self.wait_req = False

            return messages

        return self.messages
            


# Path: binance-api-manager/MarketDataFetcher.py
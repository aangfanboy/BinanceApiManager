import threading
import websocket
import logging  
import time

class MarketDataFetcher:
    def __init__(self, type = 'futures', symbol = 'btcusdt'):
        self.ws = None
        self.thread = None
        self.running = False
        self.wait_req = False
        self.type = type
        self.symbol = symbol

        self.messages = []

    def start(self):
        if self.type == 'futures':
            self.ws = websocket.WebSocketApp(f"wss://fstream.binance.com/ws/{self.symbol}@trade", on_message=self.on_message, on_error=self.on_error, on_close=self.stop)
        else:
            self.ws = websocket.WebSocketApp(f"wss://stream.binance.com:9443/ws/{self.symbol}@trade", on_message=self.on_message, on_error=self.on_error, on_close=self.stop)

        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()
        self.running = True

    def on_error(self, ws, error):
        logging.error(error)

    def on_message(self, ws, message):
        while self.wait_req:
            time.sleep(0.1)

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
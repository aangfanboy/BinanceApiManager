import sys
sys.path.append("..")

from BinanceApiManager import binance_api_manager

import tensorflow as tf
import logging
import tqdm

def setup_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DatasetCreator:
    @staticmethod
    def _float_feature(value):
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))
    
    @staticmethod
    def _bytes_feature(value):
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))
    
    @staticmethod
    def _int64_feature(value):
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))    

    def __init__(self, symbol, from_unix_time, until_unix_time, divider, type, tfrecord_path):
        self.symbol = symbol
        self.from_unix_time = from_unix_time
        self.until_unix_time = until_unix_time
        self.divider = divider
        self.type = type
        self.tfrecord_path = tfrecord_path

        self.tfrecord_writer = tf.io.TFRecordWriter(self.tfrecord_path)
        logging.info(f"Created tfrecord file at {self.tfrecord_path}")

    def add_to_tfrecord(self, input_list):
        feature = {
            't': self._int64_feature(input_list[0]),
            'p': self._float_feature(input_list[1]),
            'q': self._float_feature(input_list[2]),
            'm': self._int64_feature(input_list[3]),
        }

        example = tf.train.Example(features=tf.train.Features(feature=feature))
        self.tfrecord_writer.write(example.SerializeToString())

    def create(self):
        fetcher = binance_api_manager.OldTradesFetcher(self.type, self.symbol, self.divider)
        fetcher.start(self.from_unix_time, self.until_unix_time)

        while fetcher.fetching:
            pass

        progress_bar = tqdm.tqdm(total=len(fetcher.messages), desc="Creating tfrecord file")

        while True:
            if len(fetcher.messages) > 0:
                self.add_to_tfrecord(fetcher.messages.pop(0))
                progress_bar.update(1)
            else:
                break

        progress_bar.close()
        self.tfrecord_writer.close()

        logging.info(f"Finished creating tfrecord file at {self.tfrecord_path}")

        return self.tfrecord_path
    

# Path: dataset_maker/DatasetCreator.py
    
if __name__ == "__main__":
    setup_logging()

    creator = DatasetCreator("BTCUSDT", 1678415245000, 1678418845000, 1000, "futures", "dataset_maker/dataset.tfrecord")
    creator.create()

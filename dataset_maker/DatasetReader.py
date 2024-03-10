import tensorflow as tf
import logging
import tqdm
import time
import numpy as np

def setup_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DatasetReader:
    def __init__(self, tfrecord_path):
        self.tfrecord_path = tfrecord_path
        self.tfrecord_dataset = tf.data.TFRecordDataset(self.tfrecord_path)
        self.messages = []

    def _parse_function(self, example_proto):
        feature_description = {
            't': tf.io.FixedLenFeature([], tf.int64),
            'p': tf.io.FixedLenFeature([], tf.float32),
            'q': tf.io.FixedLenFeature([], tf.float32),
            'm': tf.io.FixedLenFeature([], tf.int64),
        }

        return tf.io.parse_single_example(example_proto, feature_description)

    def read(self):
        parsed_dataset = self.tfrecord_dataset.map(self._parse_function)

        progress_bar = tqdm.tqdm(desc="Reading tfrecord file")

        for parsed_record in parsed_dataset:
            progress_bar.update(1)
            self.messages.append(
                [
                    parsed_record['t'].numpy(),
                    parsed_record['p'].numpy(),
                    parsed_record['q'].numpy(),
                    parsed_record['m'].numpy(),
                ]
            )

        progress_bar.close()
        
        return self.messages
    
    def read_as_dataset(self):
        return self.tfrecord_dataset.map(self._parse_function)
 
if __name__ == "__main__":
    setup_logging()

    reader = DatasetReader("dataset_maker/dataset.tfrecord")
    messages = reader.read()
    print(messages)

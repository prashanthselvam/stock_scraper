import logging
import os

from google.cloud import storage

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class GCLMover():

    def __init__(self, dst_bucket):
        self.client = storage.Client()
        self.bucket = self.client.bucket(dst_bucket)
        self.paths = ['data/stocks/',
                      'data/raw_data/',
                      'data/final_output/',
                      'analysis_output/']

    def upload_files(self, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to {} in {} bucket.".format(
                source_file_name, destination_blob_name, self.bucket
            )
        )

    def move_files(self):
        for path in self.paths:
            for f in os.listdir(path):
                name = path + f
                self.upload_files(name, name)
                os.remove(name)
                logging.info('{} deleted'.format(name))

    def run(self):
        self.move_files()


if __name__ == '__main__':
    GCLMover('stock_scrape_bucket').run()

import logging
import os

from google.cloud import storage

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class GCLMover():

    def __init__(self, dst_bucket, cron_path=''):
        self.client = storage.Client()
        self.bucket = self.client.bucket(dst_bucket)
        self.cron_path = cron_path
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
            path = os.path.join(self.cron_path, path)
            for f in os.listdir(path):
                name = path + f
                self.upload_files(name, name)
                os.remove(name)
                logging.info('{} deleted'.format(name))

    def run(self):
        self.move_files()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', default='stock_scrape_bucket')
    parser.add_argument('-c', default='')
    args = parser.parse_args()

    dest_bucket = args.b
    cron_path = args.c

    GCLMover(dest_bucket, cron_path).run()

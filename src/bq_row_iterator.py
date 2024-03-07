from google.cloud.bigquery_storage import BigQueryReadClient
from multicorn.utils import log_to_postgres, ERROR, WARNING, INFO, DEBUG


class BigQueryRowIterator(object):
    def __init__(self, query_result, max_queue_size=10, bqstorage_client=None):
        self.rows_processed = 0
        self.total_rows = query_result.total_rows
        self.arrow_iterable = query_result.to_arrow_iterable(
            max_queue_size=max_queue_size,
            bqstorage_client=bqstorage_client or BigQueryReadClient(),
        )
        self.next_batch()

    def next_batch(self):
        log_to_postgres(
            f"Percent of rows processed: {self.rows_processed / self.total_rows:.2%}.",
            DEBUG,
        )
        self.current_batch = next(self.arrow_iterable).to_pylist()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.current_batch:
            self.rows_processed = self.rows_processed + 1
            return self.current_batch.pop(0)
        else:
            self.next_batch()
            return self.next()

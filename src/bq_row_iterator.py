from google.cloud.bigquery_storage import BigQueryReadClient


class BigQueryRowIterator(object):
    def next_batch(self):
        self.current_batch = next(self.arrow_iterable).to_pylist()

    def __init__(self, query_result, max_queue_size=2, bqstorage_client=None):
        self.arrow_iterable = query_result.to_arrow_iterable(
            max_queue_size=max_queue_size,
            bqstorage_client=bqstorage_client or BigQueryReadClient(),
        )
        self.next_batch()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.current_batch:
            return self.current_batch.pop(0)
        else:
            self.next_batch()
            return self.next()

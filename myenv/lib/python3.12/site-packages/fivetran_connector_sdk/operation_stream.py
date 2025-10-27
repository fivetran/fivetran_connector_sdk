import queue
import threading

from fivetran_connector_sdk.constants import (
    QUEUE_SIZE,
    MAX_RECORDS_IN_BATCH,
    MAX_BATCH_SIZE_IN_BYTES,
    CHECKPOINT_OP_TIMEOUT_IN_SEC
)
from fivetran_connector_sdk.protos import connector_sdk_pb2


class _OperationStream:
    """
    A simple iterator-based stream backed by a queue for producing and consuming operations.

    This class allows adding data items into a queue and consuming them using standard iteration.
    It uses a sentinel object to signal the end of the stream.

    Example:
        stream = _OperationStream()
        stream.add("response1")
        stream.mark_done()

        for response in stream:
            print(response)  # prints "response1"
    """

    def __init__(self):
        """
        Initializes the operation stream with a queue and a sentinel object.
        """
        self._queue = queue.Queue(maxsize=QUEUE_SIZE)
        self._sentinel = object()
        self._is_done = False
        self._buffer = []
        self._buffer_record_count = 0
        self._buffer_size_bytes = 0
        self._checkpoint_lock = threading.Lock()
        self._checkpoint_flush_signal = threading.Event()
        self._checkpoint_flush_signal.set()

    def __iter__(self):
        """
        Returns the iterator instance itself.
        """
        return self

    def add(self, operation):
        """
        Adds an operation to the stream. Guarantees that operations within a single thread are processed in the order.

        In multithreaded environment if a thread initiates a checkpoint, it's producer is blocked until the
        checkpoint flush is complete. This block is localized, other threads
        remain unblocked and can continue to perform other operations
        (such as upserts, updates, deletes), but they are prevented from initiating a new checkpoint
        until the existing one is finished.

        Args:
            operation (object): The data item to add to the stream.
        """
        if isinstance(operation, connector_sdk_pb2.Checkpoint):
            # lock to ensure checkpoint operations are processed one at a time
            with self._checkpoint_lock:
                # clear the signal to indicate checkpoint operation is being processed.
                self._checkpoint_flush_signal.clear()
                self._queue.put(operation)
                # wait until the consumer flushes the buffer and sets the flag.
                if not self._checkpoint_flush_signal.wait(CHECKPOINT_OP_TIMEOUT_IN_SEC):
                    raise TimeoutError(
                        "Checkpoint flush timed out. Consumer may have failed to process checkpoint."
                    )
        else:
            self._queue.put(operation)

    def unblock(self):
        """
        Unblocks the queue, called by consumer after the checkpoint flush is completed.
        """
        self._checkpoint_flush_signal.set()

    def mark_done(self):
        """
        Marks the end of the stream by putting a sentinel in the queue.
        """
        self._queue.put(self._sentinel)

    def __next__(self):
        """
        Retrieves the next item from the stream. Raises StopIteration when the sentinel is encountered.

        Returns:
            object: The next item in the stream.

        Raises:
            StopIteration: If the sentinel object is encountered.
        """
        # If stream is completed and buffer is empty, raise StopIteration. Else flush the buffer.
        if self._is_done and not self._buffer:
            raise StopIteration

        if self._is_done:
            return self._flush_buffer()

        return self._build_next_batch()

    def _build_next_batch(self):
        """
        Core logic to build the batch. The loop continues until the buffer is full,
        but can be interrupted by a checkpoint or a sentinel from the producer.

        Returns:
            connector_sdk_pb2.UpdateResponse or list[connector_sdk_pb2.UpdateResponse]: Either a single response
            containing records or checkpoint, or a list of responses when flushing data with a checkpoint.

        """
        while self._buffer_record_count < MAX_RECORDS_IN_BATCH and self._buffer_size_bytes < MAX_BATCH_SIZE_IN_BYTES:
            operation = self._queue.get()

            # Case 1: If operation is sentinel, mark the stream as done, flush the buffer.
            if operation is self._sentinel:
                self._is_done = True
                if self._buffer:
                    return self._flush_buffer()
                else:
                    raise StopIteration

            # Case 2: if operation is a Checkpoint, flush the buffer and send the checkpoint.
            elif isinstance(operation, connector_sdk_pb2.Checkpoint):
                return self._flush_buffer_on_checkpoint(operation)

            # it is record, buffer it to flush in batches
            self._buffer_record_count += 1
            self._buffer_size_bytes += len(operation.SerializeToString())
            self._buffer.append(operation)

        # Case 3: If buffer size limit is reached, flush the buffer and return the response.
        return self._flush_buffer()

    def _flush_buffer_on_checkpoint(self, checkpoint: connector_sdk_pb2.Checkpoint):
        """
        Creates the responses containing the checkpoint and buffered records.

        Args:
            checkpoint (object): Checkpoint operation to be added to the response.
        """
        responses = []

        if self._buffer:
            responses.append(self._flush_buffer())

        responses.append(connector_sdk_pb2.UpdateResponse(checkpoint=checkpoint))
        return responses

    def _flush_buffer(self):
        """
        Flushes the current buffer and returns a response containing the buffered records.

        Returns:
            connector_sdk_pb2.UpdateResponse: A response containing the buffered records.
        """
        batch_to_flush = self._buffer
        self._buffer = []
        self._buffer_record_count = 0
        self._buffer_size_bytes = 0
        return connector_sdk_pb2.UpdateResponse(records=connector_sdk_pb2.Records(records=batch_to_flush))
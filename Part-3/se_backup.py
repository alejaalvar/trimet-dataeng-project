
# se_backup.py

import json
import gzip
import os
import signal
import time
import logging
from datetime import datetime, timezone
from threading import Lock, Event, Thread

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
SUBSCRIPTION_ID = 'se_backup_sub'
IDLE_SECONDS = 300
ARCHIVE_DIR = 'se_archives'

lock = Lock()
stop_event = Event()


def reset_stats():
    """Return empty statistics for a new batch."""
    return {
        'first_received': None,
        'last_received': None,
        'first_monotonic': None,
        'last_monotonic': None,
        'vehicle_ids': set(),
        'record_count': 0,
        'bytes_before_compression': 0,
        'date': None
    }


stats = reset_stats()


def callback(message):
    """Append one StopEvent to the batch's working file and update statistics.

    A message that is not a JSON object is logged and acked, because
    redelivering it can never succeed. Any other failure (e.g. a failed
    write) is nacked so Pub/Sub delivers the message again.
    """
    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    # UnicodeDecodeError and JSONDecodeError are both ValueErrors.
    try:
        record = json.loads(message.data.decode('utf-8'))

        if not isinstance(record, dict):
            raise ValueError('Expected JSON object')

    except ValueError as error:
        logging.warning('Invalid StopEvent message: %s', error)
        message.ack()
        return

    try:
        with lock:
            # The whole batch goes into the file of the day its first record
            # arrived, so a batch running past UTC midnight is not split. The
            # date is only kept once a write succeeds; otherwise a failed
            # first write would pin every later batch to that day.
            date = stats['date'] or received_at.strftime('%Y-%m-%d')

            # Records are kept as JSON Lines until the batch ends, then
            # compressed into the daily archive by compress_records().
            filename = os.path.join(
                ARCHIVE_DIR,
                f'se_{date}.jsonl'
            )

            line = json.dumps(record) + '\n'

            with open(filename, 'a', encoding='utf-8') as file:
                file.write(line)
                file.flush()
                os.fsync(file.fileno())

            stats['date'] = date

            if stats['first_received'] is None:
                stats['first_received'] = received_at
                stats['first_monotonic'] = received_mono

            stats['last_received'] = received_at
            stats['last_monotonic'] = received_mono

            stats['record_count'] += 1

            vehicle_id = record.get('vehicle_number')

            if vehicle_id is not None:
                stats['vehicle_ids'].add(vehicle_id)

            stats['bytes_before_compression'] += len(
                line.encode('utf-8')
            )

        message.ack()

    except Exception:
        logging.exception('Error processing StopEvent message')
        message.nack()


def compress_records(date):
    """Compress the day's JSON Lines file into se_<date>.json.gz as a JSON array.

    The .jsonl file is kept, so a later batch on the same day is appended
    to it and the archive is rebuilt with every record of the day. Returns
    (compressed_bytes, finish_time), or (None, None) if there is no file.
    """
    source_file = os.path.join(
        ARCHIVE_DIR,
        f'se_{date}.jsonl'
    )

    output_file = os.path.join(
        ARCHIVE_DIR,
        f'se_{date}.json.gz'
    )

    temp_file = output_file + '.tmp'

    if not os.path.exists(source_file):
        return None, None

    with open(source_file, 'r', encoding='utf-8') as source:
        with gzip.open(temp_file, 'wt', encoding='utf-8') as target:
            target.write('[\n')

            first = True

            for line in source:
                if not line.strip():
                    continue

                if not first:
                    target.write(',\n')

                target.write(line.strip())
                first = False

            target.write('\n]')

    # Replacing in one step means a crash mid-compression never leaves a
    # truncated archive in place of the previous one.
    os.replace(temp_file, output_file)

    compressed_bytes = os.path.getsize(output_file)
    finish_time = datetime.now(timezone.utc)

    return compressed_bytes, finish_time


def log_statistics(compressed_bytes, finish_time, elapsed):
    """Log the statistics of a finished batch."""
    count = stats['record_count']

    throughput = count / elapsed if elapsed > 0 else 0

    logging.info('========== STOPEVENT BACKUP STATISTICS ==========')

    logging.info(
        'First record received: %s',
        stats['first_received'].isoformat()
    )

    logging.info(
        'Unique vehicles: %d',
        len(stats['vehicle_ids'])
    )

    logging.info('Total StopEvent records: %d', count)

    logging.info(
        'Last record received: %s',
        stats['last_received'].isoformat()
    )

    logging.info(
        'Bytes before compression: %d',
        stats['bytes_before_compression']
    )

    logging.info(
        'Compressed file size: %d bytes',
        compressed_bytes
    )

    logging.info(
        'Compression finished: %s',
        finish_time.isoformat()
    )

    logging.info(
        'Elapsed wall-clock time: %.3f seconds',
        elapsed
    )

    logging.info(
        'Throughput: %.2f records/second',
        throughput
    )

    logging.info(
        'Archive file: se_%s.json.gz',
        stats['date']
    )

    logging.info('===============================================')


def finish_batch_locked():
    """Compress the current batch into its daily archive, log statistics and start a new batch.

    The caller must hold `lock`. Does nothing if no records were received.
    """
    global stats

    if stats['record_count'] == 0:
        return

    compress_start = time.monotonic()
    compressed_bytes, finish_time = compress_records(stats['date'])
    compress_seconds = time.monotonic() - compress_start

    # Without its working file the batch can never be archived; starting
    # over avoids retrying (and failing) every few seconds forever.
    if compressed_bytes is None:
        logging.error(
            'Working file for se_%s is missing; batch of %d records not archived',
            stats['date'], stats['record_count']
        )
        stats = reset_stats()
        return

    # The batch is only known to be over after IDLE_SECONDS of silence, so
    # that wait is excluded, as in backup.py: elapsed = receive time +
    # compression time.
    receive_seconds = stats['last_monotonic'] - stats['first_monotonic']
    log_statistics(compressed_bytes, finish_time, receive_seconds + compress_seconds)

    stats = reset_stats()


def finish_batch():
    """Finish the current batch now (see finish_batch_locked)."""
    with lock:
        finish_batch_locked()


def finish_if_idle():
    """Finish the batch once no message has arrived for IDLE_SECONDS.

    Pub/Sub has no end-of-batch signal, so a long silence is taken to mean
    the publisher is done for the day. The check and the finish happen under
    one hold of the lock so a message arriving in between cannot be added to
    the batch being closed.
    """
    with lock:
        if stats['record_count'] == 0:
            return

        idle_time = time.monotonic() - stats['last_monotonic']

        if idle_time >= IDLE_SECONDS:
            logging.info('StopEvent batch idle for %.0f seconds.', idle_time)
            finish_batch_locked()


def check_batch_completion():
    """Check every 5 seconds whether the batch has gone idle, until shutdown."""
    while not stop_event.wait(5):
        try:
            finish_if_idle()

        except Exception:
            logging.exception('Error compressing StopEvent archive')


def handle_stop_signal(signum, frame):
    """Turn Ctrl+C or systemd's SIGTERM into one KeyboardInterrupt so the batch is archived.

    The VM shuts down at the end of its daily window and systemd stops this
    service with SIGTERM, whose Python default exits at once without the
    final finish_batch(). Later stop signals are ignored so a second Ctrl+C
    or SIGTERM cannot interrupt the shutdown itself and lose the batch.
    """
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    raise KeyboardInterrupt


def main():
    """Archive StopEvents from se_backup_sub until stopped, one daily file per batch."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(
        PROJECT_ID, SUBSCRIPTION_ID
    )

    checker = Thread(
        target=check_batch_completion,
        daemon=True
    )

    checker.start()

    logging.info(
        'StopEvent backup subscriber listening on %s',
        sub_path
    )

    logging.info(
        'Batch inactivity threshold: %d seconds',
        IDLE_SECONDS
    )

    streaming_pull = None

    try:
        while not stop_event.is_set():
            streaming_pull = subscriber.subscribe(
                sub_path,
                callback=callback,
                await_callbacks_on_shutdown=True
            )

            try:
                streaming_pull.result()

            except Exception:
                logging.exception(
                    'Connection interrupted. Reconnecting...'
                )

                streaming_pull.cancel()
                time.sleep(5)

    # Caught here rather than around result() alone, because the stop signal
    # can arrive anywhere in the loop, including the reconnect sleep.
    # KeyboardInterrupt is not an Exception, so the handler above skips it.
    except KeyboardInterrupt:
        logging.info('Stopping StopEvent backup subscriber...')

    finally:
        stop_event.set()

        # The stop can arrive anywhere above, e.g. during the reconnect
        # sleep. cancel() does not wait for running callbacks; with
        # await_callbacks_on_shutdown=True this result() does, so no record
        # is written after the final finish_batch() and left unarchived.
        # A failure here (e.g. the network already going down at VM
        # shutdown) must not stop the batch from being archived below.
        try:
            if streaming_pull is not None:
                streaming_pull.cancel()

                try:
                    streaming_pull.result()
                except Exception:
                    pass

            # Closed only after running callbacks finish, so their acks are sent.
            subscriber.close()

        except Exception:
            logging.exception('Error shutting down the Pub/Sub subscriber')

        # Archive whatever was received so a clean shutdown does not leave
        # the batch uncompressed.
        try:
            finish_batch()

        except Exception:
            logging.exception('Failed to finalize StopEvent backup')


if __name__ == '__main__':
    main()

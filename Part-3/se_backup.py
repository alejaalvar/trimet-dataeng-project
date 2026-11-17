
# se_backup.py

import json
import gzip
import os
import time
import logging
from datetime import datetime, timezone
from threading import Lock, Event, Thread

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
TOPIC_ID = 'se_topic'
SUBSCRIPTION_ID = 'se_backup_sub'
IDLE_SECONDS = 300

# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

lock = Lock()
stop_event = Event()


# Initialize daily statistics
def reset_stats():
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


# Archive directory
ARCHIVE_DIR = 'se_archives'
os.makedirs(ARCHIVE_DIR, exist_ok=True)


# Process incoming StopEvent messages
def callback(message):
    global stats

    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
        record = json.loads(message.data.decode('utf-8'))

        if not isinstance(record, dict):
            raise ValueError('Expected JSON object')

        with lock:
            # Determine the date for the current batch
            if stats['date'] is None:
                stats['date'] = received_at.strftime('%Y-%m-%d')

            date = stats['date']

            # Write records to a temporary JSON Lines file
            # before compressing at the end of the batch
            filename = os.path.join(
                ARCHIVE_DIR,
                f'se_{date}.jsonl'
            )

            line = json.dumps(record) + '\n'

            with open(filename, 'a', encoding='utf-8') as file:
                file.write(line)
                file.flush()
                os.fsync(file.fileno())

            # Record first receive time
            if stats['first_received'] is None:
                stats['first_received'] = received_at
                stats['first_monotonic'] = received_mono

            stats['last_received'] = received_at
            stats['last_monotonic'] = received_mono

            stats['record_count'] += 1

            # Track unique vehicles
            vehicle_id = record.get('VEHICLE_ID')

            if vehicle_id is not None:
                stats['vehicle_ids'].add(vehicle_id)

            stats['bytes_before_compression'] += len(
                line.encode('utf-8')
            )

        message.ack()

    except Exception:
        logging.exception('Error processing StopEvent message')
        message.nack()


# Compress daily StopEvent records
def compress_records(date):
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

    # Compress into a valid JSON array
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

    # Atomically replace the daily compressed archive
    os.replace(temp_file, output_file)

    compressed_bytes = os.path.getsize(output_file)
    finish_time = datetime.now(timezone.utc)

    return compressed_bytes, finish_time


# Log completed batch statistics
def log_statistics(compressed_bytes, finish_time):
    count = stats['record_count']

    if count == 0:
        return

    elapsed = (
        stats['last_monotonic']
        - stats['first_monotonic']
    )

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
        'Elapsed receive time: %.3f seconds',
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


# Check for batch completion after inactivity
def check_batch_completion():
    global stats

    while not stop_event.wait(5):
        with lock:
            if stats['record_count'] == 0:
                continue

            idle_time = (
                time.monotonic()
                - stats['last_monotonic']
            )

            if idle_time >= IDLE_SECONDS:
                date = stats['date']

                try:
                    compressed_bytes, finish_time = compress_records(date)

                    if compressed_bytes is not None:
                        log_statistics(compressed_bytes, finish_time)

                        logging.info(
                            'StopEvent backup complete after %.0f seconds idle.',
                            idle_time
                        )

                        stats = reset_stats()

                except Exception:
                    logging.exception('Error compressing StopEvent archive')


# Start background inactivity checker
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


# Long-running subscriber
try:
    with subscriber:
        while not stop_event.is_set():
            streaming_pull = subscriber.subscribe(
                sub_path,
                callback=callback
            )

            try:
                streaming_pull.result()

            except KeyboardInterrupt:
                logging.info('Stopping StopEvent backup subscriber...')
                stop_event.set()
                streaming_pull.cancel()
                break

            except Exception:
                logging.exception(
                    'Connection interrupted. Reconnecting...'
                )

                streaming_pull.cancel()
                time.sleep(5)

finally:
    stop_event.set()

    # Compress remaining records during a clean shutdown
    with lock:
        if stats['record_count'] > 0:
            try:
                compressed_bytes, finish_time = compress_records(
                    stats['date']
                )

                if compressed_bytes is not None:
                    log_statistics(compressed_bytes, finish_time)

            except Exception:
                logging.exception('Failed to finalize StopEvent backup')

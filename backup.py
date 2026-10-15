
# backup.py
import json
import time
import os
import gzip
import shutil
import logging
from datetime import datetime, timezone
from threading import Lock, Event, Thread

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
SUBSCRIPTION_ID = 'backup_sub'

# Wait 5 minutes without messages if no sentinel arrives
IDLE_SECONDS = 300

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


def reset_stats():
    """Initialize statistics for the next batch."""
    return {
        'first_received': None,
        'first_monotonic': None,
        'last_monotonic': None,
        'vehicle_ids': set(),
        'breadcrumb_count': 0,
        'expected_count': None,
        'sentinel_received': False,
        'file': None,
        'filename': None
    }


stats = reset_stats()


def open_backup_file(received_at):
    """Create a daily backup file."""

    date_str = received_at.strftime('%Y-%m-%d')
    filename = f'breadcrumbs_{date_str}_.log'

    # Avoid overwriting a previous completed batch
    if os.path.exists(filename) or os.path.exists(
        filename + '.gz'
    ):
        batch = 2
        while (
            os.path.exists(
                f'breadcrumbs_{date_str}_{batch}.log'
            )
            or os.path.exists(
                f'breadcrumbs_{date_str}_{batch}.log.gz'
            )
        ):
            batch += 1

        filename = f'breadcrumbs_{date_str}_{batch}.log'

    stats['filename'] = filename
    stats['file'] = open(filename, 'ab')

    logging.info('Started backup: %s', filename)


def log_statistics(file_size, compressed_at, elapsed):
    """Log completed backup statistics."""

    count = stats['breadcrumb_count']

    throughput = (
        count / elapsed if elapsed > 0 else 0
    )

    logging.info('========== BACKUP STATISTICS ==========')

    logging.info(
        'First breadcrumb received: %s',
        stats['first_received'].isoformat()
    )

    logging.info(
        'Total breadcrumbs received: %d',
        count
    )

    logging.info(
        'Bytes received (file size): %d',
        file_size
    )

    logging.info(
        'Unique vehicles: %d',
        len(stats['vehicle_ids'])
    )

    logging.info(
        'Compression completed: %s',
        compressed_at.isoformat()
    )

    logging.info(
        'Elapsed wall-clock time: %.3f seconds',
        elapsed
    )

    logging.info(
        'Throughput: %.2f breadcrumbs/second',
        throughput
    )

    logging.info(
        'Compressed file: %s.gz',
        stats['filename']
    )

    logging.info('=======================================')


def finish_backup():
    """Close, compress, and report the completed batch."""
    global stats

    if stats['breadcrumb_count'] == 0:
        return

    filename = stats['filename']
    compressed_filename = filename + '.gz'
    temporary_filename = compressed_filename + '.tmp'

    # Flush and close the original backup file
    if stats['file'] is not None:
        stats['file'].flush()
        stats['file'].close()
        stats['file'] = None

    # Record uncompressed backup size
    file_size = os.path.getsize(filename)

    # Compress without loading entire file into memory
    with open(filename, 'rb') as source:
        with gzip.open(temporary_filename, 'wb') as dest:
            shutil.copyfileobj(source, dest)

    # Replace temporary file with completed archive
    os.replace(
        temporary_filename,
        compressed_filename
    )

    # Remove uncompressed file after successful compression
    os.remove(filename)

    # Timestamp after compression finishes
    compressed_at = datetime.now(timezone.utc)

    elapsed = (
        time.monotonic()
        - stats['first_monotonic']
    )

    log_statistics(
        file_size,
        compressed_at,
        elapsed
    )

    # Reset for next batch
    stats = reset_stats()


def callback(message):
    """Receive and back up Pub/Sub messages."""
    global stats

    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
        payload = json.loads(
            message.data.decode('utf-8')
        )

        if not isinstance(payload, dict):
            raise ValueError('Expected JSON object')

        with lock:

            # Sentinel indicates expected breadcrumb count
            if payload.get('type') == 'sentinel':

                stats['sentinel_received'] = True
                stats['expected_count'] = int(
                    payload['message_count']
                )

                if stats['expected_count'] < 0:
                    raise ValueError('Invalid message count')

                # Check whether all records have arrived
                if (
                    stats['breadcrumb_count']
                    >= stats['expected_count']
                ):
                    finish_backup()

            else:

                # Validate vehicle ID
                vehicle_id = payload.get('VEHICLE_ID')

                if vehicle_id is None:
                    raise ValueError('Missing VEHICLE_ID')

                # First breadcrumb opens the daily file
                if stats['first_received'] is None:
                    open_backup_file(received_at)

                    stats['first_received'] = received_at
                    stats['first_monotonic'] = received_mono

                # Write original JSON directly to disk
                stats['file'].write(message.data + b'\n')
                stats['file'].flush()

                # Update aggregate statistics
                stats['breadcrumb_count'] += 1
                stats['vehicle_ids'].add(vehicle_id)
                stats['last_monotonic'] = received_mono

                # A sentinel can arrive before the final record
                if (
                    stats['sentinel_received']
                    and stats['expected_count'] is not None
                    and stats['breadcrumb_count']
                    >= stats['expected_count']
                ):
                    finish_backup()

        message.ack()

    except (ValueError, KeyError, TypeError) as error:
        logging.warning('Invalid message: %s', error)
        message.ack()

    except Exception:
        logging.exception('Backup processing error')
        message.nack()


def check_batch_completion():
    """
    Fallback for publishers without sentinel messages.
    Finish after IDLE_SECONDS without new breadcrumbs.
    """
    global stats

    while not stop_event.wait(5):

        with lock:

            if stats['breadcrumb_count'] == 0:
                continue

            # Once sentinel arrives, wait for expected count
            if stats['sentinel_received']:
                continue

            idle_time = (
                time.monotonic()
                - stats['last_monotonic']
            )

            if idle_time >= IDLE_SECONDS:

                logging.info(
                    'No messages for %.0f seconds.',
                    idle_time
                )

                try:
                    finish_backup()
                except Exception:
                    logging.exception(
                        'Failed to finish backup'
                    )


def main():
    logging.info(
        'Backup subscriber listening on %s',
        sub_path
    )

    # Background thread checks completion
    checker = Thread(
        target=check_batch_completion,
        daemon=True
    )
    checker.start()

    try:
        with subscriber:

            # Keep listening indefinitely
            while not stop_event.is_set():

                streaming_pull = subscriber.subscribe(
                    sub_path,
                    callback=callback
                )

                try:
                    streaming_pull.result()

                except KeyboardInterrupt:
                    logging.info('Stopping backup...')
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

        with lock:
            if stats['file'] is not None:
                stats['file'].flush()
                stats['file'].close()


if __name__ == '__main__':
    main()

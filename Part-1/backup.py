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


# Initialize statistics
def reset_stats():
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


# Create daily backup file
def open_backup_file(received_at):

    date_str = received_at.strftime('%Y-%m-%d')
    filename = f'breadcrumbs_{date_str}_.log'

    # Prevent overwriting earlier backups
    if (
        os.path.exists(filename)
        or os.path.exists(filename + '.gz')
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


# Log completed backup statistics
def log_statistics(file_size, compressed_at, elapsed):

    count = stats['breadcrumb_count']

    throughput = count / elapsed if elapsed > 0 else 0

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


# Close and compress backup file
def finish_backup():
    global stats

    if stats['breadcrumb_count'] == 0:
        return

    filename = stats['filename']
    compressed_filename = filename + '.gz'
    temporary_filename = compressed_filename + '.tmp'

    # Flush and close original file
    if stats['file'] is not None:
        stats['file'].flush()
        stats['file'].close()
        stats['file'] = None

    # Record file size before compression
    file_size = os.path.getsize(filename)

    # Compress file in chunks
    with open(filename, 'rb') as source:
        with gzip.open(temporary_filename, 'wb') as dest:
            shutil.copyfileobj(source, dest)

    os.replace(
        temporary_filename,
        compressed_filename
    )

    # Remove original uncompressed file
    os.remove(filename)

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


# Process incoming Pub/Sub messages
def callback(message):
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

            # Check for sentinel message
            if payload.get('type') == 'sentinel':

                expected = int(payload['message_count'])

                if expected < 0:
                    raise ValueError('Invalid message count')

                stats['sentinel_received'] = True
                stats['expected_count'] = expected

                # Finish if all breadcrumbs have arrived
                if (
                    stats['breadcrumb_count']
                    >= stats['expected_count']
                ):
                    finish_backup()

            else:

                vehicle_id = payload.get('VEHICLE_ID')

                if vehicle_id is None:
                    raise ValueError('Missing VEHICLE_ID')

                # First breadcrumb starts a new batch
                if stats['first_received'] is None:

                    open_backup_file(received_at)

                    stats['first_received'] = received_at
                    stats['first_monotonic'] = received_mono

                # Append raw JSON to file
                stats['file'].write(message.data + b'\n')
                stats['file'].flush()

                # Update statistics
                stats['breadcrumb_count'] += 1
                stats['vehicle_ids'].add(vehicle_id)
                stats['last_monotonic'] = received_mono

                # Sentinel may arrive before last breadcrumb
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
        logging.exception('Error processing message')
        message.nack()


# Check whether batch has finished
def check_batch_completion():
    global stats

    while not stop_event.wait(5):

        with lock:

            if stats['breadcrumb_count'] == 0:
                continue

            idle_time = (
                time.monotonic()
                - stats['last_monotonic']
            )

            if idle_time >= IDLE_SECONDS:

                # sentinel arrived but we never reached its count,
                # meaning some breadcrumbs were invalid or never delivered
                if (
                    stats['sentinel_received']
                    and stats['expected_count'] is not None
                    and stats['breadcrumb_count'] < stats['expected_count']
                ):
                    logging.warning(
                        'Finishing with %d of %d expected breadcrumbs',
                        stats['breadcrumb_count'],
                        stats['expected_count']
                    )

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


# Start background inactivity checker
checker = Thread(
    target=check_batch_completion,
    daemon=True
)

checker.start()

logging.info(
    'Backup subscriber listening on %s',
    sub_path
)

logging.info(
    'Batch inactivity threshold: %d seconds',
    IDLE_SECONDS
)


# Start subscriber directly (no main function)
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


# analysis.py
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from threading import Lock, Event, Thread

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
SUBSCRIPTION_ID = 'analysis_sub'

# Wait 5 minutes after the last message
# before treating the batch as complete.
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
    """Reset statistics for the next batch."""
    return {
        'first_received': None,
        'last_received': None,
        'first_monotonic': None,
        'last_monotonic': None,
        'earliest_breadcrumb': None,
        'latest_breadcrumb': None,
        'vehicle_ids': set(),
        'trip_ids': set(),
        'breadcrumb_count': 0,
        'invalid_count': 0
    }


stats = reset_stats()


def parse_breadcrumb_time(payload):
    """Combine OPD_DATE and ACT_TIME."""

    opd_date = str(payload['OPD_DATE']).strip()
    act_time = payload['ACT_TIME']

    # Support common date formats
    date = None

    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d-%b-%y'):
        try:
            date = datetime.strptime(opd_date, fmt)
            break
        except ValueError:
            continue

    if date is None:
        raise ValueError(f'Unknown date format: {opd_date}')

    # ACT_TIME may be seconds after midnight
    # or a HH:MM:SS formatted string.
    if isinstance(act_time, str) and ':' in act_time:
        parts = list(map(int, act_time.split(':')))
        seconds = (
            parts[0] * 3600
            + parts[1] * 60
            + parts[2]
        )
    else:
        seconds = int(act_time)

    return date + timedelta(seconds=seconds)


def log_statistics():
    """Print statistics for the completed batch."""

    count = stats['breadcrumb_count']

    if count == 0:
        return

    elapsed = (
        stats['last_monotonic']
        - stats['first_monotonic']
    )

    throughput = count / elapsed if elapsed > 0 else 0

    logging.info('========== BATCH STATISTICS ==========')

    logging.info(
        'First breadcrumb received: %s',
        stats['first_received'].isoformat()
    )

    logging.info(
        'Unique vehicles: %d',
        len(stats['vehicle_ids'])
    )

    logging.info(
        'Earliest breadcrumb: %s',
        stats['earliest_breadcrumb']
    )

    logging.info(
        'Latest breadcrumb: %s',
        stats['latest_breadcrumb']
    )

    logging.info(
        'Unique trips: %d',
        len(stats['trip_ids'])
    )

    logging.info(
        'Total breadcrumbs: %d',
        count
    )

    logging.info(
        'Last message received: %s',
        stats['last_received'].isoformat()
    )

    logging.info(
        'Active receive time: %.3f seconds',
        elapsed
    )

    logging.info(
        'Throughput: %.2f breadcrumbs/second',
        throughput
    )

    logging.info(
        'Invalid records: %d',
        stats['invalid_count']
    )

    logging.info('======================================')


def callback(message):
    """Process each incoming breadcrumb."""
    global stats

    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
        payload = json.loads(
            message.data.decode('utf-8')
        )

        if not isinstance(payload, dict):
            raise ValueError('Expected JSON object')

        # Extract only the fields needed
        vehicle_id = payload.get('VEHICLE_ID')
        trip_id = payload.get('EVENT_NO_TRIP')

        breadcrumb_time = parse_breadcrumb_time(payload)

        if vehicle_id is None or trip_id is None:
            raise ValueError('Missing vehicle or trip ID')

        with lock:

            # Track first breadcrumb arrival
            if stats['first_received'] is None:
                stats['first_received'] = received_at
                stats['first_monotonic'] = received_mono

            # Track last breadcrumb arrival
            stats['last_received'] = received_at
            stats['last_monotonic'] = received_mono

            # Count breadcrumbs
            stats['breadcrumb_count'] += 1

            # Track unique vehicles and trips
            stats['vehicle_ids'].add(vehicle_id)
            stats['trip_ids'].add(trip_id)

            # Update earliest breadcrumb timestamp
            if (
                stats['earliest_breadcrumb'] is None
                or breadcrumb_time < stats['earliest_breadcrumb']
            ):
                stats['earliest_breadcrumb'] = breadcrumb_time

            # Update latest breadcrumb timestamp
            if (
                stats['latest_breadcrumb'] is None
                or breadcrumb_time > stats['latest_breadcrumb']
            ):
                stats['latest_breadcrumb'] = breadcrumb_time

        # Acknowledge successfully processed message
        message.ack()

    except (ValueError, KeyError, TypeError, OverflowError):
        with lock:
            stats['invalid_count'] += 1
        # Skip malformed records rather than retry forever
        message.ack()

    except Exception:
        logging.exception('Unexpected processing error')
        message.nack()


def check_batch_completion():
    """
    Check whether the publisher appears finished.

    A batch is considered complete after
    IDLE_SECONDS without receiving a breadcrumb.
    """
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

                log_statistics()

                logging.info(
                    'Batch complete after %.0f seconds idle.',
                    idle_time
                )

                # Reset for next day's batch
                stats = reset_stats()


def main():
    logging.info(
        'Listening on %s',
        sub_path
    )

    logging.info(
        'Batch inactivity threshold: %d seconds',
        IDLE_SECONDS
    )

    # Start background completion checker
    checker = Thread(
        target=check_batch_completion,
        daemon=True
    )
    checker.start()

    # Subscriber runs continuously
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
                    logging.info('Stopping subscriber...')
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


if __name__ == '__main__':
    main()

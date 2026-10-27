# analysis.py

import json
import time
import logging
import psycopg2
import os
from datetime import datetime, timedelta, timezone
from threading import Lock, Event, Thread

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
SUBSCRIPTION_ID = 'analysis_sub'
IDLE_SECONDS = 300  # 5 minutes means the day's run is done

# PostgreSQL configuration
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "breadcrumbs"
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASSWORD"]

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)

conn.autocommit = True

# Lock for database operations
db_lock = Lock()

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)

logging.basicConfig(
    level=logging.INFO,  # the threshold for what severity of message gets logged
    format='%(asctime)s - %(levelname)s - %(message)s'  # layout of each entry
)

# we prevent race conditions on shared global statistics using a lock
lock = Lock()
stop_event = Event()  # used to cleanly shut down background processes


# Initialize statistics
def reset_stats():
    return {
        'first_received': None,
        'last_received': None,
        'first_monotonic': None,
        'last_monotonic': None,
        'earliest_breadcrumb': None,
        'latest_breadcrumb': None,
        'vehicle_ids': set(),  # sets enforce uniqueness
        'trip_ids': set(),
        'breadcrumb_count': 0,
        'invalid_count': 0
    }

# because we track stats day by day without restarting the program,
# we need to wipe the dictionary at the end of each daily run. this
# global variable is what we will be modifying
stats = reset_stats()


# Parse TriMet breadcrumb timestamp
def parse_breadcrumb_time(payload):

    date = datetime.strptime(
        payload['OPD_DATE'],
        '%d%b%Y:%H:%M:%S'
    )

    seconds = int(payload['ACT_TIME'])

    return date + timedelta(seconds=seconds)

# Insert valid breadcrumb into PostgreSQL
def insert_breadcrumb(payload, breadcrumb_time):

    sql = """
        INSERT INTO breadcrumb
        (trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        int(payload['EVENT_NO_TRIP']),
        int(payload['VEHICLE_ID']),
        breadcrumb_time,
        float(payload['GPS_LATITUDE']),
        float(payload['GPS_LONGITUDE']),
        float(payload['VELOCITY']) if payload.get('VELOCITY') is not None else None,
        int(payload['METERS']) if payload.get('METERS') is not None else None
    )

    with db_lock:
        with conn.cursor() as cur:
            cur.execute(sql, values)


# Log statistics for completed batch
# this is run after 5 minutes of inactivity, signaling
# that all messages have been received and processed
def log_statistics():

    count = stats['breadcrumb_count']

    # avoid logging an empty report
    if count == 0:
        return

    elapsed = (
        stats['last_monotonic']
        - stats['first_monotonic']
    )

    throughput = count / elapsed if elapsed > 0 else 0

    logging.info('========== ANALYSIS STATISTICS ==========')

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
        'Last breadcrumb received: %s',
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

    logging.info('=========================================')


# Process incoming Pub/Sub messages
def callback(message):
    global stats

    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
      # decode the raw bytes and convert to python dict
        payload = json.loads(
            message.data.decode('utf-8')
        )

        if not isinstance(payload, dict):
            raise ValueError('Expected JSON object')

        vehicle_id = payload.get('VEHICLE_ID')
        trip_id = payload.get('EVENT_NO_TRIP')

        breadcrumb_time = parse_breadcrumb_time(payload)

        # handle incomplete data gracefully
        if vehicle_id is None or trip_id is None:
            raise ValueError('Missing vehicle or trip ID')

        insert_breadcrumb(payload, breadcrumb_time)

        # acquire the lock before touching global stats
        with lock:

            # checking if this is the very first message of the day
            if stats['first_received'] is None:
                stats['first_received'] = received_at
                stats['first_monotonic'] = received_mono

            stats['last_received'] = received_at
            stats['last_monotonic'] = received_mono

            stats['breadcrumb_count'] += 1

            stats['vehicle_ids'].add(vehicle_id)
            stats['trip_ids'].add(trip_id)

            # is the current earliest breadcrumb value actually the earliest?
            if (
                stats['earliest_breadcrumb'] is None
                or breadcrumb_time < stats['earliest_breadcrumb']
            ):
                stats['earliest_breadcrumb'] = breadcrumb_time

            # is the current latest breadcrumb value actually the latest?
            if (
                stats['latest_breadcrumb'] is None
                or breadcrumb_time > stats['latest_breadcrumb']
            ):
                stats['latest_breadcrumb'] = breadcrumb_time

        # processing succeeded as normal, make sure the message is removed
        # from the queue
        message.ack()

    except (ValueError, KeyError, TypeError, OverflowError):
        with lock:
            stats['invalid_count'] += 1

        # prevent bad data from clogging the queue by deleting it
        message.ack()

    except Exception:
        logging.exception('Error processing message')
        message.nack()


# Check for completion after inactivity
# this function gets run in a background thread, checker
def check_batch_completion():
    global stats

    # every 5 secs we ask, how long since we received the last message?
    while not stop_event.wait(5):

        with lock:

            if stats['breadcrumb_count'] == 0:
                continue

            idle_time = (
                time.monotonic()
                - stats['last_monotonic']
            )

            # if 5 minutes has elapsed, the publisher is finished
            if idle_time >= IDLE_SECONDS:

                log_statistics()

                logging.info(
                    'Batch complete after %.0f seconds idle.',
                    idle_time
                )

                # clear the dict for the next day's run
                stats = reset_stats()


# Start background inactivity checker
checker = Thread(
    target=check_batch_completion,
    daemon=True
)

checker.start()

logging.info(
    'Analysis subscriber listening on %s',
    sub_path
)

logging.info(
    'Batch inactivity threshold: %d seconds',
    IDLE_SECONDS
)


# this is our long running execution block
try:
    with subscriber:

        while not stop_event.is_set():

            # establish the connection to pub-sub
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

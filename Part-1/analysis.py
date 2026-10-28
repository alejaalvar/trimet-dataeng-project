import json
import time
import logging
import os
import math
from datetime import datetime, timedelta, timezone
from threading import Lock, Event, Thread

import psycopg2
from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
SUBSCRIPTION_ID = 'analysis_sub'
IDLE_SECONDS = 300
SPEED_LIMIT_KMH = 120
INVALID_DIR = '.'

# PostgreSQL configuration
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'breadcrumbs'
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASSWORD']

conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                        user=DB_USER, password=DB_PASS)
conn.autocommit = True

db_lock = Lock()
lock = Lock()
stop_event = Event()
subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def reset_stats():
    return {
        'first_received': None, 'last_received': None,
        'first_monotonic': None, 'last_monotonic': None,
        'earliest_breadcrumb': None, 'latest_breadcrumb': None,
        'vehicle_ids': set(), 'trip_ids': set(),
        'breadcrumb_count': 0, 'invalid_count': 0
    }


stats = reset_stats()
# The trip state is tracked for accepted records only.
# Each trip has its assigned vehicle and latest accepted timestamp/distance.
trip_state = {}
last_message_monotonic = None


def parse_breadcrumb_time(record):
    date = datetime.strptime(record['OPD_DATE'], '%d%b%Y:%H:%M:%S')
    return date + timedelta(seconds=int(record['ACT_TIME']))


def require_number(value, field):
    if isinstance(value, bool) or value is None:
        raise ValueError(f'{field} must be numeric')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f'{field} must be finite')
    return result


def calculate_speed(record, breadcrumb_time, previous):
    """Speed in m/s relative to the latest accepted earlier trip record.

    First record of a trip has speed 0.0. Out-of-order records with no
    earlier accepted reference have speed 0.0 provisionally.
    """
    if previous is None:
        return 0.0
    delta_seconds = (breadcrumb_time - previous['timestamp']).total_seconds()
    if delta_seconds <= 0:
        return 0.0
    current_meters = require_number(record['METERS'], 'METERS')
    delta_meters = current_meters - previous['meters']
    return delta_meters / delta_seconds


def validate_record(record):
    """Return (violations, parsed timestamp, calculated speed).

    Caller holds the main lock so per-trip state is consistent.
    Every violation excludes the record from database insertion.
    """
    violations = []
    vehicle_id = record.get('VEHICLE_ID')
    trip_id = record.get('EVENT_NO_TRIP')
    act_time = record.get('ACT_TIME')

    if vehicle_id is None or str(vehicle_id).strip() == '':
        violations.append('A3: VEHICLE_ID must exist')
    if trip_id is None or str(trip_id).strip() == '':
        violations.append('A4: EVENT_NO_TRIP must exist')

    valid_time = (
        not isinstance(act_time, bool) and
        (isinstance(act_time, int) or
         (isinstance(act_time, str) and act_time.isascii() and act_time.isdecimal()))
    )
    if not valid_time or int(act_time) < 0:
        violations.append('A5: ACT_TIME must be a non-negative integer')

    if violations:
        return violations, None, None

    try:
        breadcrumb_time = parse_breadcrumb_time(record)
        meters = require_number(record.get('METERS'), 'METERS')
        # Ensure identifiers can be inserted into the database.
        int(vehicle_id)
        int(trip_id)
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        violations.append(f'Invalid field for transformation: {error}')
        return violations, None, None

    previous = trip_state.get(str(trip_id))
    if previous is not None and str(vehicle_id) != previous['vehicle_id']:
        violations.append('A10: Records in the same trip must have the same VEHICLE_ID')

    try:
        speed = calculate_speed(record, breadcrumb_time, previous)
        if speed > SPEED_LIMIT_KMH / 3.6:
            violations.append(f'A14: Calculated speed {speed * 3.6:.2f} km/h exceeds 120 km/h')
        if speed < 0:
            violations.append('Distance decreased within a trip (negative calculated speed)')
    except (ValueError, TypeError, OverflowError) as error:
        violations.append(f'Speed transformation failed: {error}')
        speed = None

    return violations, breadcrumb_time, speed


def save_invalid_record(record, violations, received_at):
    """Append to a valid daily JSON array (called while lock is held)."""
    filename = os.path.join(INVALID_DIR, f'invalid_data_{received_at:%Y-%m-%d}.json')
    entry = {'record': record, 'violations': violations,
             'received_at': received_at.isoformat()}
    if not os.path.exists(filename):
        with open(filename, 'w', encoding='utf-8') as file:
            file.write('[]')

    with open(filename, 'r+', encoding='utf-8') as file:
        file.seek(0, os.SEEK_END)
        size = file.tell()
        if size < 2:
            raise ValueError('Invalid daily JSON file')
        file.seek(size - 1)
        if file.read(1) != ']':
            raise ValueError('Daily JSON file is not an array')
        file.seek(size - 1)
        file.write(',\n' if size > 2 else '\n')
        json.dump(entry, file, default=str)
        file.write('\n]')
        file.truncate()

    for violation in violations:
        logging.warning('VALIDATION VIOLATION [%s] | record: %s', violation, record)


def transform_record(record, breadcrumb_time, speed):
    """Sections 5.2-5.5: remove, create, and rename fields before loading."""
    transformed = record.copy()

    # 5.2: Drop unneeded fields.
    for field in ('EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP'):
        transformed.pop(field, None)

    # 5.3: Create timestamp and remove its source fields.
    transformed['timestamp'] = breadcrumb_time
    transformed.pop('OPD_DATE', None)
    transformed.pop('ACT_TIME', None)

    # 5.4: Speed in meters per second from cumulative trip distance.
    transformed['speed'] = speed
    transformed.pop('VELOCITY', None)  # Do not load the original velocity.

    # 5.5: Rename fields to match the PostgreSQL schema.
    rename = {
        'EVENT_NO_TRIP': 'trip_id', 'VEHICLE_ID': 'vehicle_id',
        'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude'
    }
    for original, new in rename.items():
        transformed[new] = transformed.pop(original)

    # PostgreSQL INSERT expects these seven fields, plus METERS.
    return transformed


def insert_breadcrumb(record):
    sql = '''
        INSERT INTO breadcrumb
        (trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    values = (
        int(record['trip_id']), int(record['vehicle_id']),
        record['timestamp'], require_number(record['latitude'], 'latitude'),
        require_number(record['longitude'], 'longitude'),
        float(record['speed']), require_number(record['METERS'], 'METERS')
    )
    with db_lock:
        with conn.cursor() as cur:
            cur.execute(sql, values)


def log_statistics():
    count = stats['breadcrumb_count']
    if count == 0 and stats['invalid_count'] == 0:
        return
    start = stats['first_monotonic']
    end = stats['last_monotonic']
    elapsed = end - start if start is not None and end is not None else 0
    throughput = count / elapsed if elapsed > 0 else 0
    logging.info('========== ANALYSIS STATISTICS ==========')
    logging.info('First breadcrumb received: %s', stats['first_received'])
    logging.info('Unique vehicles: %d', len(stats['vehicle_ids']))
    logging.info('Earliest breadcrumb: %s', stats['earliest_breadcrumb'])
    logging.info('Latest breadcrumb: %s', stats['latest_breadcrumb'])
    logging.info('Unique trips: %d', len(stats['trip_ids']))
    logging.info('Total breadcrumbs loaded: %d', count)
    logging.info('Last breadcrumb received: %s', stats['last_received'])
    logging.info('Active receive time: %.3f seconds', elapsed)
    logging.info('Throughput: %.2f breadcrumbs/second', throughput)
    logging.info('Invalid records: %d', stats['invalid_count'])
    logging.info('=========================================')


def callback(message):
    global last_message_monotonic
    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
        try:
            payload = json.loads(message.data.decode('utf-8'))
        except (ValueError, UnicodeError) as error:
            payload = message.data.decode('utf-8', errors='replace')
            parse_error = f'Malformed JSON or encoding: {error}'
        else:
            parse_error = None if isinstance(payload, dict) else 'Expected JSON object'

        with lock:
            if parse_error:
                violations = [parse_error]
                breadcrumb_time = speed = None
            else:
                violations, breadcrumb_time, speed = validate_record(payload)

            if violations:
                save_invalid_record(payload, violations, received_at)
                stats['invalid_count'] += 1
            else:
                transformed = transform_record(payload, breadcrumb_time, speed)
                # Convert/check every required database field before insertion.
                try:
                    insert_breadcrumb(transformed)
                except (ValueError, TypeError, KeyError, OverflowError) as error:
                    save_invalid_record(payload, [f'Database field conversion: {error}'], received_at)
                    stats['invalid_count'] += 1
                else:
                    if stats['first_received'] is None:
                        stats['first_received'] = received_at
                        stats['first_monotonic'] = received_mono
                    stats['last_received'] = received_at
                    stats['last_monotonic'] = received_mono
                    stats['breadcrumb_count'] += 1
                    stats['vehicle_ids'].add(transformed['vehicle_id'])
                    stats['trip_ids'].add(transformed['trip_id'])
                    if stats['earliest_breadcrumb'] is None or breadcrumb_time < stats['earliest_breadcrumb']:
                        stats['earliest_breadcrumb'] = breadcrumb_time
                    if stats['latest_breadcrumb'] is None or breadcrumb_time > stats['latest_breadcrumb']:
                        stats['latest_breadcrumb'] = breadcrumb_time

                    key = str(payload['EVENT_NO_TRIP'])
                    previous = trip_state.get(key)
                    if previous is None:
                        trip_state[key] = {
                            'vehicle_id': str(payload['VEHICLE_ID']),
                            'timestamp': breadcrumb_time,
                            'meters': require_number(payload['METERS'], 'METERS')
                        }
                    elif breadcrumb_time > previous['timestamp']:
                        previous['timestamp'] = breadcrumb_time
                        previous['meters'] = require_number(payload['METERS'], 'METERS')
            last_message_monotonic = received_mono

        message.ack()
    except Exception:
        logging.exception('Error processing message; retrying')
        message.nack()


def check_batch_completion():
    global stats, trip_state, last_message_monotonic
    while not stop_event.wait(5):
        with lock:
            if last_message_monotonic is None:
                continue
            idle_time = time.monotonic() - last_message_monotonic
            if idle_time >= IDLE_SECONDS:
                log_statistics()
                logging.info('Batch complete after %.0f seconds idle.', idle_time)
                stats = reset_stats()
                trip_state = {}
                last_message_monotonic = None


checker = Thread(target=check_batch_completion, daemon=True)
checker.start()
logging.info('Analysis subscriber listening on %s', sub_path)
logging.info('Batch inactivity threshold: %d seconds', IDLE_SECONDS)

try:
    with subscriber:
        while not stop_event.is_set():
            streaming_pull = subscriber.subscribe(sub_path, callback=callback)
            try:
                streaming_pull.result()
            except KeyboardInterrupt:
                logging.info('Stopping subscriber...')
                stop_event.set()
                streaming_pull.cancel()
                break
            except Exception:
                logging.exception('Connection interrupted. Reconnecting...')
                streaming_pull.cancel()
                time.sleep(5)
finally:
    stop_event.set()
    conn.close()

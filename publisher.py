# publisher.py

import collections
import json
import time
import requests
from google.cloud import pubsub_v1
import uuid
from datetime import datetime, timezone

# setup GCP credentials and routing
PUBLISHER_VM_IP     = '8.229.127.245'
PUBLISHER_VM_REGION = 'us-west1 (Oregon)'
PROJECT_ID = 'dataeng-project-511504'
TOPIC_ID = 'bc_topic'

# Vehicle IDs to fetch
VEHICLE_IDS = [
4208, 2925, 4228, 3649, 3629, 4522, 3914, 4016, 3328, 3564, 3002, 3250, 3031, 3253, 3556, 3801, 4029, 3232,
3756, 3902, 2931, 3406, 3960, 4209, 3421, 3304, 4070, 3239, 4505, 4058, 3134, 4020, 3954, 3008, 3637, 3612,
3216, 3114, 3568, 3222, 4234, 4519, 3035, 3538, 4516, 3732, 3943, 3913, 3543, 3725, 3263, 2916, 3713, 4215,
3053, 3207, 3941, 4064, 3204, 3211, 3308, 2915, 3719, 3011, 4230, 3736, 4517, 3422, 3634, 3754, 3126, 3302,
3162,  3303, 3531, 3165, 3734, 3547, 3235, 3905, 3315, 4531, 3416, 3016, 3911, 3015, 4212, 3005, 3241, 3948,
4028, 4304, 4005, 2934, 3958, 3220, 3613, 3323, 3573, 3961, 4021, 3621, 3571, 3518, 3153, 3313, 3262, 3419,
3319, 4237, 3548, 3615, 3529, 3018, 3009, 3945, 3907, 3127, 3605, 3004, 3927, 3118, 4067, 3906, 4026, 3109,
3507,  3160, 3950, 3408, 3243, 2912, 3534, 4515, 3569, 3639, 2932, 3757, 3228, 3708, 3113, 4061, 3716, 4043,
4050, 3052, 3233, 3321, 3745, 3036, 3715, 3722, 3505, 3120, 3322, 4048, 4031, 3509, 3411, 3102, 3647, 3626,
3137, 3312, 3116, 3245, 3632, 2902, 3023, 3608, 4018, 4004, 3712, 2938, 4526, 4053, 4504, 3112, 2920, 3937,
3726, 3206, 4037, 3230, 3566, 3537, 4014, 3115, 4051, 3413, 4015, 4010, 4305, 3146, 3723, 3130, 3542, 4059,
3012, 3208, 3227, 2901, 3054, 4211, 3214, 4219, 3609, 3741, 3549, 3029, 3156, 3740, 3519, 3750, 3403, 3414,
3201, 3933, 3928, 3577, 3219, 4227, 3735, 3254, 3922, 3919, 4055, 3103, 3059, 2927, 4502, 4206, 3148, 3407,
3729,  3320, 3523,3947, 3959, 3748, 4013, 3912, 3264, 3648, 4012, 3257, 3217, 4040, 3915, 4001
]

API_URL = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs'
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

total_breadcrumbs = 0
publishing_errors = 0

# we use a set because it provides uniqueness guarantees, as well as O(1) lookup
# and insertion time due to Python's underlying implementation. we use this var
# for pipeline statistics so we need it to be efficient
vehicles_with_data = set()
run_id = str(uuid.uuid4())
start_time = time.perf_counter()
api_start_timestamp = datetime.now(timezone.utc)
final_publish_timestamp = None

# allows TCP connection pooling to prevent overhead of re-establishing a
# connection for each of the 250 vehicles
session = requests.Session()

for vehicle in VEHICLE_IDS:
  try:
    # we attempt to get the breadcrumbs up to 3 times
    for attempt in range(3):
        try:
            response = session.get(API_URL, params={'vehicle_id': vehicle}, timeout=10)
            break
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
            # exponential backoff handles network hiccups cleanly
            time.sleep(2 * (attempt + 1))

    response.raise_for_status()

  # keep going if that breadcrumb gave us a status error
  except requests.RequestException:
      continue

  # we got the breadcrumbs, now try to extract the JSON payload
  try:
    records = response.json()
    # the response may return a dictionary or a raw list, so we check for that
    records = records.get('results') if isinstance(records, dict) else records
  except ValueError:  # if the server does not return valid json, .json throws
    continue

  # skip to the next vehicle on invalid input
  if not isinstance(records, list):
    continue

  # empty lists are falsy - catch false positives or empty responses
  if records:
    vehicles_with_data.add(vehicle)

  # our input is validated - time to serialize the record and publish
  for record in records:
    try:
      message = json.dumps(record).encode('utf-8')
      future = publisher.publish(topic_path, message)
      future.result()  # blocking call, wait for confirmation synchronously

      final_publish_timestamp = datetime.now(timezone.utc)

      total_breadcrumbs += 1

    # catch almost any error during the publish process gracefully
    except Exception:
      publishing_errors += 1

  time.sleep(0.1)   # be polite to the server

# remember to close the session since we kept a connection pool
session.close()
end_time = time.perf_counter()
elapsed_time = end_time - start_time
throughput = total_breadcrumbs / elapsed_time if elapsed_time > 0 else 0

print("\nPublisher Statistics:")
print("--------------------")
print(f"RunID: {run_id}")
print(f"API Start Timestamp: {api_start_timestamp}")
print(f"Final Publish Timestamp: {final_publish_timestamp}")
print(f"Elapsed Wall-Clock Time: {elapsed_time:.2f} seconds")
print(f"Total Breadcrumbs Published: {total_breadcrumbs}")
print(f"Publishing Throughput: {throughput:.2f} breadcrumbs/second")
print(f"Unique Vehicles With Data: {len(vehicles_with_data)}")
print(f"Publishing Errors: {publishing_errors}")

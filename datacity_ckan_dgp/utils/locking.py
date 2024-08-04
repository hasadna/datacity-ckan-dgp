import os
import uuid
import json
import glob
import time
import datetime
from contextlib import contextmanager


LOCK_TTL_SECONDS = 3600  # 1 hour
WAIT_TTL_SECONDS = 60 * 10  # 10 minutes
BASE_LOCK_PATH = os.getenv('BASE_LOCK_PATH', '/var/ckan_dgp_locks')


def is_my_lock_active(my_lock_id, lock_path):
    locks_to_remove = []
    valid_locks = {}
    for lock_file in glob.glob(os.path.join(lock_path, f'*.json')):
        with open(lock_file) as f:
            lock = json.load(f)
        lock_id = lock['id']
        lock_time = datetime.datetime.strptime(lock['time'], '%Y-%m-%d %H:%M:%S:%f')
        if lock_time < datetime.datetime.now() - datetime.timedelta(seconds=LOCK_TTL_SECONDS):
            locks_to_remove.append(lock_file)
        else:
            valid_locks[f'{lock_time.strftime("%Y-%m-%d %H:%M:%S:%f")}_{lock_id}'] = lock_id
    active_lock_key = list(sorted(valid_locks.keys()))[0] if len(valid_locks) > 0 else None
    active_lock_id = valid_locks[active_lock_key] if active_lock_key else None
    if active_lock_id == my_lock_id:
        for lock_file in locks_to_remove:
            os.remove(lock_file)
        return True
    else:
        return False


@contextmanager
def instance_package_lock(instance_name, package_id, with_lock=True):
    if with_lock:
        lock_id = str(uuid.uuid4())
        lock_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        lock_file = os.path.join(BASE_LOCK_PATH, instance_name, package_id, f'{lock_id}.json')
        os.makedirs(os.path.dirname(lock_file), exist_ok=True)
        with open(lock_file, 'w') as f:
            json.dump({
                'id': lock_id,
                'time': lock_time,
            }, f)
        start_wait_time = datetime.datetime.now()
        while True:
            if is_my_lock_active(lock_id, os.path.dirname(lock_file)):
                break
            if datetime.datetime.now() - start_wait_time > datetime.timedelta(seconds=WAIT_TTL_SECONDS):
                os.remove(lock_file)
                raise Exception(f'Failed to acquire lock for {instance_name}/{package_id} after {WAIT_TTL_SECONDS} seconds')
            time.sleep(1)
        try:
            yield
        finally:
            os.remove(lock_file)
    else:
        yield

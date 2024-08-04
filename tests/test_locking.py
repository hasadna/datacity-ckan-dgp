import os
import uuid
import json
import glob
import tempfile
import datetime

import pytest

from datacity_ckan_dgp.utils.locking import is_my_lock_active, LOCK_TTL_SECONDS, instance_package_lock


def create_lock_file(lockdir, my_lock_time=None):
    my_lock_id = str(uuid.uuid4())
    if my_lock_time is None:
        my_lock_time = datetime.datetime.now()
    with open(f'{lockdir}/{my_lock_id}.json', 'w') as f:
        json.dump({
            'id': my_lock_id,
            'time': my_lock_time.strftime('%Y-%m-%d %H:%M:%S:%f')
        }, f)
    return my_lock_id


def test_is_my_lock_active_no_locks():
    with tempfile.TemporaryDirectory() as tmpdir:
        my_lock_id = create_lock_file(tmpdir)
        assert is_my_lock_active(my_lock_id, tmpdir)


def test_is_my_lock_active_older_lock_exists():
    with tempfile.TemporaryDirectory() as tmpdir:
        older_lock_id = create_lock_file(tmpdir, datetime.datetime.now() - datetime.timedelta(seconds=1))
        my_lock_id = create_lock_file(tmpdir)
        assert not is_my_lock_active(my_lock_id, tmpdir)
        os.remove(f'{tmpdir}/{older_lock_id}.json')
        assert is_my_lock_active(my_lock_id, tmpdir)


def test_is_my_lock_active_delete_older_expired_lock():
    with tempfile.TemporaryDirectory() as tmpdir:
        expired_lock_id = create_lock_file(tmpdir, datetime.datetime.now() - datetime.timedelta(seconds=LOCK_TTL_SECONDS+1))
        my_lock_id = create_lock_file(tmpdir)
        assert is_my_lock_active(my_lock_id, tmpdir)
        assert not os.path.exists(f'{tmpdir}/{expired_lock_id}.json')


def test_is_my_lock_active_ignore_newer_lock():
    with tempfile.TemporaryDirectory() as tmpdir:
        create_lock_file(tmpdir, datetime.datetime.now() + datetime.timedelta(seconds=5))
        my_lock_id = create_lock_file(tmpdir)
        assert is_my_lock_active(my_lock_id, tmpdir)


def test_is_my_lock_active_same_time():
    with tempfile.TemporaryDirectory() as tmpdir:
        my_lock_time = datetime.datetime.now()
        my_lock_id = create_lock_file(tmpdir, my_lock_time)
        other_lock_id = create_lock_file(tmpdir, my_lock_time)
        assert is_my_lock_active(my_lock_id, tmpdir) == (my_lock_id < other_lock_id)


def test_instance_package_lock():
    from datacity_ckan_dgp.utils import locking
    with tempfile.TemporaryDirectory() as tmpdir:
        locking.BASE_LOCK_PATH = tmpdir
        locking.WAIT_TTL_SECONDS = 2
        with instance_package_lock('test_instance', 'test_package'):
            assert len(glob.glob(f'{tmpdir}/test_instance/test_package/*.json')) == 1
            with pytest.raises(Exception, match='Failed to acquire lock for test_instance/test_package after 2 seconds'):
                with instance_package_lock('test_instance', 'test_package'):
                    pass
        assert len(glob.glob(f'{tmpdir}/test_instance/test_package/*.json')) == 0

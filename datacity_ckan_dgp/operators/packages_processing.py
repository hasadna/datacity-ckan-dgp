import traceback
from datacity_ckan_dgp import ckan
from datacity_ckan_dgp.utils.locking import instance_package_lock


def operator(name, params, only_package_id=None, with_lock=True):
    instance_name = params['instance_name']
    task = params['task']
    if task == "geojson":
        from datacity_ckan_dgp.package_processing_tasks.geojson import process_package
    elif task == "xlsx":
        from datacity_ckan_dgp.package_processing_tasks.xlsx import process_package
    else:
        raise Exception("Unknown processing task: {}".format(task))
    num_errors = 0
    for package_id in ckan.package_list(instance_name):
        if only_package_id and package_id != only_package_id:
            continue
        try:
            with instance_package_lock(instance_name, package_id, with_lock):
                process_package(instance_name, package_id)
        except:
            traceback.print_exc()
            num_errors += 1
    return num_errors == 0


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)

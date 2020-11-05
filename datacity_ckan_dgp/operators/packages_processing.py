import traceback
from datacity_ckan_dgp import ckan


def operator(name, params):
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
        try:
            process_package(instance_name, package_id)
        except:
            traceback.print_exc()
            num_errors += 1
    return num_errors == 0


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)

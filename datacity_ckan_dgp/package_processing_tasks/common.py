from datacity_ckan_dgp import ckan


def process_package(instance_name, package_id, task_id, is_resource_valid_for_processing, process_resource):
    package = ckan.package_show(instance_name, package_id)
    for resource in package['resources']:
        if is_resource_valid_for_processing(instance_name, package, resource):
            package_extras_processed_res = "processed_res_{}_{}".format(task_id, resource['id'])
            package_extras = {e['key']: e['value'] for e in package.get('extras', [])}
            if package_extras.get(package_extras_processed_res) == "yes":
                print("Already processed {} ({} > {} > {} {})".format(task_id, instance_name, package['name'], resource['name'], resource['id']))
            else:
                print("Starting {} processing ({} > {} > {} {})".format(task_id, instance_name, package['name'], resource['name'], resource['id']))
                process_resource(instance_name, package, resource, package_extras_processed_res)
                print("OK")


def update_package_extras(instance_name, package, package_extras_processed_res):
    package = ckan.package_show(instance_name, package['id'])
    package.setdefault('extras', []).append({"key": package_extras_processed_res, "value": "yes"})
    ckan.package_update(instance_name, package)

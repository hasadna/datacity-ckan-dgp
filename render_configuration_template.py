import os
import sys
import json

instance_names = [
    k[14:-4].lower().replace('_', ' ').capitalize()
    for k in os.environ.keys()
    if k.startswith('CKAN_INSTANCE_') and k.endswith('_URL')
]
template_filename = sys.argv[1]
with open(template_filename) as f:
    print(f.read().replace('["__CKAN_INSTANCES__"]', json.dumps(instance_names)))

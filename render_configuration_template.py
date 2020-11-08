import os
import sys
import json

def extract_from_env(prefix, suffix, use_values=False):
    return [
        dict(
            display=k[len(prefix):-len(suffix)].lower().replace('_', ' ').capitalize(),
            value=v if use_values else k[len(prefix):-len(suffix)]
        )
        for k, v in os.environ.items()
        if k.startswith(prefix) and k.endswith(suffix)
    ]

ckan_instance_names = extract_from_env('CKAN_INSTANCE_', '_URL')
db_instance_names = extract_from_env('DB_INSTANCE_', '_URL', use_values=True)

template_filename = sys.argv[1]
with open(template_filename) as f:
    print(
        f.read()
         .replace('["__CKAN_INSTANCES__"]', json.dumps(ckan_instance_names))
         .replace('["__DB_INSTANCES__"]', json.dumps(db_instance_names))
    )

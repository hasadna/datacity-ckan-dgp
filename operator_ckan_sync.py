from datacity_ckan_dgp.operators import ckan_sync


def operator(name, params):
    ckan_sync.operator(name, params)

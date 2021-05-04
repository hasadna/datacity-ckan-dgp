mock_calls = []


def update_package_extras(*args, **kwargs):
    mock_calls.append(('update_package_extras', args, kwargs))

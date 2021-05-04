mock_calls = []


def resource_create(*args, **kwargs):
    if kwargs.get('files'):
        kwargs['files'] = {
            file_name: file.read() for file_name, file in kwargs['files']
        }
    mock_calls.append(('resource_create', args, kwargs))

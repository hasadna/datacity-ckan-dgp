from datacity_ckan_dgp.package_processing_tasks import geojson


def test_lat_lon():
    row = {'X_ITM': '182418', 'Y_ITM': 652418}
    assert geojson.get_lat_lon_values(row, 'X_ITM', 'Y_ITM') == (34.81195612163422, 31.963835962756367)

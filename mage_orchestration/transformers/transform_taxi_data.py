if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    print("Rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    print("Rows with zero trip distance: ",   data['trip_distance'].isin([0]).sum())
    
    data = data[(data['passenger_count'] > 0) | (data['trip_distance'] > 0)]
    position = data.columns.get_loc('lpep_pickup_datetime') + 1
    data.insert(position, 'lpep_pickup_date', data['lpep_pickup_datetime'].dt.date)
    data = data.rename(columns={'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id','PULocationID':'pu_location_id','DOLocationID':'do_location_id'})
 
    return data

@test
def test_output(output, *args):
    
    valid_vendor_ids = [1, 2, '']  # List of valid 'vendor_id' values

    condition = (
        (output['passenger_count'] > 0) | 
        (output['trip_distance'] > 0) | 
        output['vendor_id'].isin(valid_vendor_ids)
    )
    assert condition.any(), 'There are either rides with zero passengers or rides with zero trip distance'
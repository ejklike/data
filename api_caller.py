"""make query string and retrieve data from google maps place api """

import time
import requests
import secret

sleep_time = 2

def get_location(data):
    """
    input: json data (len==1)
    output: lat, lng
    """
    assert len(data) == 2
    location = data[0]['geometry']['location']
    return location['lat'], location['lng']

def remove_duplicate(data_list):
    """remove duplicated items"""
    data_dict = {}
    for data in data_list:
        key = data['place_id']
        data_dict[key] = data
    return list(data_dict.values())

def get_data(query_url):
    """
    input: query_url
    output: list of json data
    """
    def _retrieve_data(query_url, timeout=100, sleep_time=sleep_time):
        data = requests.get(url=query_url, timeout=timeout)
        data = data.json()
        status = data['status']
        time.sleep(sleep_time)
        return data, status

    _result = []
    _original_query = query_url
    while query_url:
        data, status = _retrieve_data(query_url)
        #append data
        if status == 'OK':
            _result += data['results']
        elif status == 'OVER_QUERY_LIMIT':
            print(status)
            while status != 'OK':
                data, status = _retrieve_data(query_url)
        elif status == 'INVALID_REQUEST':
            print(status)
            print(query_url)
            while status != 'OK':
                # time.sleep(sleep_time)
                data, status = _retrieve_data(query_url)
        #next query url
        if 'next_page_token' in data.keys():
            query_url = '{}&pagetoken={}'.format(
                _original_query,
                data['next_page_token']
            )
        else:
            query_url = None
    return _result, status


class NearbySearchCaller(object):
    """
    caller = NearbySearchCaller(types=types)
    json_list = caller.run_query(lat, lng, radius)
    """
    def __init__(self, types=(), language='en'):
        self.root_url = (
            'https://maps.googleapis.com/maps/api/place/'
            'nearbysearch/json?key={}&language={}'
        ).format(secret.api_key, language)
        if len(types) > 0:
            self.types = '|'.join(types)
            self.root_url += '&types={}'.format(self.types)
        self.depth = 0

    def run_recursive_query(self, lat, lng, radius=500):
        """run recursive query"""
        self.depth += 1
        _result = []
        radius = radius / 2
        interval = radius / (60*60*30) #meter to sec
        radius = max(int(radius), 1)

        this_lat_list = [
            lat + interval, lat + interval,
            lat - interval, lat - interval
        ]
        this_lng_list = [
            lng + interval, lng - interval,
            lng + interval, lng - interval
        ]
        for i, (lat, lng) in enumerate(zip(this_lat_list, this_lng_list), 1):
            query_result, status = self.run_query(lat, lng, radius, init=False)
            print(self.depth*' ', self.depth, '-', i, lat, lng, radius, status)
            _result += query_result
        _result = remove_duplicate(_result)
        self.depth += -1
        return _result

    def run_query(self, lat, lng, radius=500, init=True):
        if init:
            self.depth = 0
        """run query to retrieve data from api"""
        _query_url = self.root_url \
            + '&location={},{}&radius={}'.format(lat, lng, radius)
        _result, status = get_data(_query_url)

        if len(_result) >= 60:
            print('****** #result exceeded 60.')
            more_result = self.run_recursive_query(lat, lng, radius)
            _result += more_result
            _result = remove_duplicate(_result)
        if status == 'OK':
            status += '(+{})'.format(len(_result))
        return _result, status

# if __name__ == '__main__':
#     place_id = 'd18c1a7fface649bc9e5fe0f08460196e504e15c'
#     api = PlaceCaller()
#     api.set_query(place_id)
#     print(api.run_query())



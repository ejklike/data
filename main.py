"""
https://developers.google.com/maps/documentation/geocoding/geocoding-strategies
https://developers.google.com/maps/documentation/geocoding/usage-limits?authuser=2

"""

from pymysql import connect, cursors
from itertools import product
import numpy as np
import api_caller
import insert_query
import secret

db_interval = 100

###
# define the radius of query
#
# second:meter = 1:30 (approx.)
# degree:meter = 1/60/60:30 (approx.)
###
unit = 30
radius = int(30 * np.sqrt(2) / 2 * unit)
radius = int(radius/100)*100 

###
# define the ranges of lat and lng
###
step = 1/60/60 * unit #1초 = about 30m or 24m
lat_rng = np.arange(35.50, 35.75, step)
lng_rng = np.arange(139.60, 139.90, step)
# lat_rng = [35.533333]
# lng_rng = [139.700000]

num_query = len(lat_rng)*len(lng_rng)
print('radius = {}m'.format(radius))
print('(#lat, #lng, #lat*lng) = ({}, {}, {})'.format(
    len(lat_rng), len(lng_rng), num_query))

types = (
    'place_of_worship',
    'book_store',
    'museum',
    'art_gallery',
    'library',
    'painter',
#    'clothing_store',
#    'department_store',
#    'jewelry_store',
#    'shoe_store',
#    'shopping_mall',
    'subway_station',
    'train_station',
    'amusement_park',
    'aquarium',
    'casino',
    'night_club',
    'park',
    'zoo',
    'stadium',
)


if __name__ == "__main__":
    #db cursor
    conn = connect(
        host=secret.mysql_host,
        user=secret.mysql_id,
        password=secret.mysql_pw,
        db=secret.mysql_db,
        charset='utf8mb4',
        cursorclass=cursors.DictCursor
    )
    cursor = conn.cursor()

    #api
    api = api_caller.NearbySearchCaller(language='en', types=types)

    #crawling
    data_dict = {} #list of crawled data
    for i, (lat, lng) in enumerate(product(lat_rng, lng_rng), 1):
        print('{:6}/{}({:5.1f}%): (lat, lng) = ({:6.6f}, {:6.6f})'.format(
            i, num_query, round(i / num_query, 2)*100, lat, lng), end=', ')
        query_result, status = api.run_query(
            lat=lat,
            lng=lng,
            radius=radius
        )
        print('status = {:13}'.format(status), end=', ')
        for data in query_result:
            key = data['place_id']
            data_dict[key] = data
        print('#data = {:5}'.format(len(data_dict)), end='')
        if i % db_interval == 0 or i == num_query:
            print('****** insert into DB...', end='\r', flush=True)
            def _execute_data(json_list):
                try:
                    cursor.executemany(
                        insert_query.query_string,
                        insert_query.preprocess(json_list)
                    )
                except Exception as ex:
                    template = (
                        'An exception of type {0} occured. '
                        'Arguments:\n{1!r}'
                    )
                    message = template.format(type(ex).__name__, ex.args)
                    print(message)
            data_list = list(data_dict.values())
            _execute_data(data_list)
            conn.commit()
            data_list = []
        print('', end='\r\n', flush=True)

    conn.close()




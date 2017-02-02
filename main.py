"""
https://developers.google.com/places/web-service/intro

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
unit = 50 # interval size
radius = int(30 * np.sqrt(2) / 2 * unit)
radius = int(radius/100)*100 

###
# define the ranges of lat and lng
###
step = 1/60/60 * unit #1초 = about 30m or 24m
# lat_rng = np.arange(35.50, 35.75, step)
lat_rng = np.arange(35.68, 35.75, step)
lng_rng = np.arange(139.60, 139.90, step)
# lat_rng = [35.533333]
# lng_rng = [139.700000]

num_query = len(lat_rng)*len(lng_rng)
print('radius = {}m'.format(radius))
print('(#lat, #lng, #lat*lng) = ({}, {}, {})'.format(
    len(lat_rng), len(lng_rng), num_query))

types = (
    # #tranportation
    # 'subway_station',
    # 'train_station',
    #regional or historical
    'synagogue',
    'place_of_worship',
    'mosque',
    'hindu_temple',
    #amusement
    'amusement_park',
    'aquarium',
    'park',
    'zoo',
    'stadium',
    'city_hall',
    ###
    # #restaurant
    # 'restaurant',
    # 'food',
    # 'meal_takeaway',
    # #bakery and cafe
    # 'bakery',
    # 'cafe',
    # #bar
    # 'bar',
    # #hotel
    # 'lodging',
    # #culture
    # 'book_store',
    # 'museum',
    # 'art_gallery',
    # 'library',
    # 'painter',
    # # store
    # 'shopping_mall',
    # 'department_store',
    # 'store', #invalid
    # # - clothing
    # 'clothing_store', #invalid
    # 'shoe_store',
    # 'jewelry_store',
    # # - eletronics and hardware
    # 'electronics_store',
    # 'hardware_store',
    # # - home goods and furniture
    # 'home_goods_store',
    # 'furniture_store',
    # # - liquor
    # 'liquor_store',
    # #adult
    # 'casino',
    # 'night_club',
)
print('types = {}'.format('|'.join(types)))

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
    data_dict = {}
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
            print('<- DB updated (up to this point)', end='\r', flush=True)
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




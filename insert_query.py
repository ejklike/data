"""insert query result to DB"""

PLACE_TYPES = [
    'accounting',
    'airport',
    'amusement_park',
    'aquarium',
    'art_gallery',
    'atm',
    'bakery',
    'bank',
    'bar',
    'beauty_salon',
    'bicycle_store',
    'book_store',
    'bowling_alley',
    'bus_station',
    'cafe',
    'campground',
    'car_dealer',
    'car_rental',
    'car_repair',
    'car_wash',
    'casino',
    'cemetery',
    'church',
    'city_hall',
    'clothing_store',
    'convenience_store',
    'courthouse',
    'dentist',
    'department_store',
    'doctor',
    'electrician',
    'electronics_store',
    'embassy',
    'establishment',
    'finance',
    'fire_station',
    'florist',
    'food',
    'funeral_home',
    'furniture_store',
    'gas_station',
    'general_contractor',
    'grocery_or_supermarket',
    'gym',
    'hair_care',
    'hardware_store',
    'health',
    'hindu_temple',
    'home_goods_store',
    'hospital',
    'insurance_agency',
    'jewelry_store',
    'laundry',
    'lawyer',
    'library',
    'liquor_store',
    'local_government_office',
    'locksmith',
    'lodging',
    'meal_delivery',
    'meal_takeaway',
    'mosque',
    'movie_rental',
    'movie_theater',
    'moving_company',
    'museum',
    'night_club',
    'painter',
    'park',
    'parking',
    'pet_store',
    'pharmacy',
    'physiotherapist',
    'place_of_worship',
    'plumber',
    'police',
    'post_office',
    'real_estate_agency',
    'restaurant',
    'roofing_contractor',
    'rv_park',
    'school',
    'shoe_store',
    'shopping_mall',
    'spa',
    'stadium',
    'storage',
    'store',
    'subway_station',
    'synagogue',
    'taxi_stand',
    'train_station',
    'travel_agency',
    'university',
    'veterinary_care',
    'zoo'
]

query_string = 'REPLACE INTO `tokyo` (id, place_id, lat, lng, name, price_level, rating, icon, vicinity, weekday_text, accounting, airport, amusement_park, aquarium, art_gallery, atm, bakery, bank, bar, beauty_salon, bicycle_store, book_store, bowling_alley, bus_station, cafe, campground, car_dealer, car_rental, car_repair, car_wash, casino, cemetery, church, city_hall, clothing_store, convenience_store, courthouse, dentist, department_store, doctor, electrician, electronics_store, embassy, establishment, finance, fire_station, florist, food, funeral_home, furniture_store, gas_station, general_contractor, grocery_or_supermarket, gym, hair_care, hardware_store, health, hindu_temple, home_goods_store, hospital, insurance_agency, jewelry_store, laundry, lawyer, library, liquor_store, local_government_office, locksmith, lodging, meal_delivery, meal_takeaway, mosque, movie_rental, movie_theater, moving_company, museum, night_club, painter, park, parking, pet_store, pharmacy, physiotherapist, place_of_worship, plumber, police, post_office, real_estate_agency, restaurant, roofing_contractor, rv_park, school, shoe_store, shopping_mall, spa, stadium, storage, store, subway_station, synagogue, taxi_stand, train_station, travel_agency, university, veterinary_care, zoo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

def preprocess(json_list):
    data_list = []
    for data in json_list:
        this_data = [
            data['id'],
            data['place_id'],
            data['geometry']['location']['lat'],
            data['geometry']['location']['lng'],
            data['name']
        ]
        for name in ['price_level', 'rating', 'icon', 'vicinity']:
            this_data.append(
                data[name] if name in data.keys() else None
            )
        def _is_weekday_txt(data):
            cond = False
            if 'opening_hours' in data.keys():
                cond = len(data['opening_hours']['weekday_text']) > 0
            return cond
        this_data.append(
            data['opening_hours']['weekday_text'] if _is_weekday_txt(data) else None
        )
        for t in PLACE_TYPES:
            this_data.append(1 if t in data['types'] else 0)
        data_list.append(this_data)
    return data_list


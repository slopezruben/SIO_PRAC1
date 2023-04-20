from sqlalchemy import types

currencyRates= {
        "edinburgh": 1.14,
        "san diego": 0.92,
        "mallorca": 1,
        "santiago": 0.001,
        "porto": 1,
        "sydney": 0.61,
        "rome": 1,
        "toronto": 0.68
}

diccionarioBathroom={
    "bathrooms": types.Integer(),
    "bathrooms_text": types.Text(),
    'bathrooms_text_id': types.Text(),
}

diccionarioProperty={
        'property_type_id': types.Text(), 
        "property_type": types.Text()
}

diccionarioHost={
        "host_id": types.Integer(),
        "host_name": types.Text(),
        "host_since": types.Text(),
        "host_location": types.Text(),
        "host_response_time": types.Text(),
        "host_response_rate": types.Text(),
        "host_acceptance_rate": types.Text(),
}

diccionarioNeighborhood={
        'neighbourhood_cleansed_id': types.Text(),
        "neighbourhood_cleansed": types.Text(),
}

diccionarioListing={
        "id": types.Text(),
        "name": types.Text(),
        "latitude": types.Numeric(),
        "longitude": types.Numeric(),
        "accommodates": types.BigInteger(),
        "bedrooms": types.Integer(),
        "beds": types.Integer(),
        "price": types.Text(),
        "price_float": types.Numeric(),
        "minimum_nights_avg_ntm": types.Numeric(),
        "maximum_nights_avg_ntm": types.Numeric(),
        "availability_30": types.Integer(),
        "availability_60": types.Integer(),
        "availability_90": types.Integer(),
        "availability_365": types.Integer(),
        "number_of_reviews": types.Integer(),
        "review_scores_rating": types.Numeric(),
        "review_scores_accuracy": types.Numeric(),
        "review_scores_cleanliness": types.Numeric(),
        "review_scores_checkin": types.Numeric(),
        "review_scores_communication": types.Numeric(),
        "review_scores_location": types.Numeric(),
        "review_scores_value": types.Numeric(),
        "reviews_per_month": types.Numeric(),
        'room_type_id': types.Text(), 
        'neighbourhood_cleansed_id': types.Text(),
        'bathrooms_text_id': types.Text(),
        'property_type_id': types.Text(), 
        'host_id': types.Text(),
        'city_id': types.Text()
}

diccionarioRoom={
    'room_type_id': types.Text(),
    'room_type': types.Text(),
}

diccionarioId={
        'room_type_id': types.Text(), 
        'neighbourhood_cleansed_id': types.Text(),
        'bathrooms_text_id': types.Text(),
        'property_type_id': types.Text(), 
}

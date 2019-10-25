"""

Example:
    $ python3 etl.py 2019-10-21_07 devel

"""

import re
import gzip
import sys
from io import BytesIO
import json
import datetime
import logging

import boto3
import dateutil.parser

from eulerian import Eulerian


def client(env='devel'):
    """Connect to Eulerian API"""
    if env == 'devel':
        site = 'preprod'
    elif env == 'live':
        site = 'preprod'  # Change to the correct value for production
    else:
        raise Exception("Invalid environment {}".format(env))

    return Eulerian(auth_token='*****************************',
                    customer='ml',
                    site=site)


def ingest_booking(eulerian, **kwargs):
    """Ingest a booking"""
    order_id = kwargs['ref']
    del kwargs['ref']
    amount = kwargs['amount']
    del kwargs['amount']
    ereplay_time = kwargs['ereplay_time']
    del kwargs['ereplay_time']
    resp = eulerian.replay(order_id, amount, ereplay_time, **kwargs)
    return resp


def modify_booking(eulerian, order_id, new_amount, ea_apiasync=None):
    """Modify a booking"""
    if ea_apiasync:
        resp = eulerian.valid(order_id, new_amount, ea_apiasync)
    else:
        resp = eulerian.valid(order_id, new_amount)
    return resp


def read_gzip(bucket, key):
    """
    Read gzipped json file and returns a dictionary.
    """
    obj = bucket.Object(key)
    body = obj.get()['Body'].read()
    gzipfile = BytesIO(body)
    gzipfile = gzip.GzipFile(fileobj=gzipfile)
    content = gzipfile.read()
    my_json = content.decode()
    return [json.loads(data) for data in my_json.split('\n') if data]


def path(date):
    """Return path to gzip files for a given hour"""
    year, month, day, hour = re.split('[-|_]', date)
    return 'CRM/Salesforce/Reservas/reservas{}/{}/{}/{}'.format(year, month, day, hour)


def field_mapping(reserva):
    """
    Returns a dictionary with Eulerian API parameters. This parameters are mapped to existing
    booking entries. View README.md file for a description.
    """
    kwargs = {}

    # Required
    try:
        kwargs['ref'] = reserva['locator']
        kwargs['amount'] = reserva['euroNetAmount']
        kwargs['ereplay_time'] = dateutil.parser.parse(reserva['createdDate']).strftime('%s')
    except KeyError as error:
        logging.error("Missing required parameter: %s", error)

    # Fixed
    kwargs['from'] = "com"
    kwargs['currency'] = "EUR"
    kwargs['profile'] = "buyer"
    kwargs['prdquantity'] = 1

    # Direct mapping
    mapping = {
        "type": "pos",
        "payment": "descripcionFormaPago",
        "uid": "customerId",
        "prdref": "hotelId",
        "prdamount": "euroNetAmount",
        "user_country_ISO_Code": "countryCode",
        "user_type": "cardGroup",
        "userRewardsType": "cardGroup",
        "orderType": "descripcionFormaPago",
        "order_adults": "totalAdultosReserva",
        "order_children": "totalNinosReserva",
        "order_nights": "numeroNochesReserva",
        "order_room_code": "roomTypeCode",
        "order_hotel_room": "roomTypeCode",
        "order_rate": "fareCode",
    }
    for key in mapping:
        try:
            kwargs[key] = reserva[mapping[key]]
        except KeyError:
            pass

    # Calculated
    calculated = {
        "order_checkin": "dateutil.parser.parse(reserva['checkIn']).strftime('%Y%m%d')",
        "order_checkout": "dateutil.parser.parse(reserva['checkOut']).strftime('%Y%m%d')",
        "order_checkin_month": "dateutil.parser.parse(reserva['checkIn']).strftime('%m')",
        "order_checkin_year": "dateutil.parser.parse(reserva['checkIn']).strftime('%Y')",
        "order_checkinWeekDay": "dateutil.parser.parse(reserva['checkIn']).strftime('%A')",
        "order_daysinAdvance": ("("
                                "dateutil.parser.parse(reserva['checkIn']) -"
                                "dateutil.parser.parse(reserva['createdDate']).replace(tzinfo=None)"
                                ").days"
                                ),
        "order_rooms": "int(reserva['roomNightsReserva']) / int(reserva['numeroNochesReserva'])"
    }
    for key in calculated:
        try:
            kwargs[key] = eval(calculated[key])
        except KeyError:
            pass

    return kwargs


def send_new_bookings(eulerian, date=datetime.datetime.now().strftime("%Y-%m-%d_%H"), env='devel'):
    """
    Read all files for a given hour and sends all bookings to Eulerian
    """

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('bucket-cdr-landing-{}'.format(env))
    keys = [obj.key for obj in bucket.objects.filter(Prefix=path(date))]

    for key in keys:
        reservas = read_gzip(bucket, key)
        for reserva in reservas:
            kwargs = field_mapping(reserva)
            response = ingest_booking(eulerian, **kwargs)
            logging.info('%s', json.dumps(kwargs))
            logging.debug('url: %s, text: %s', response.url, response.text)


def main():
    """
    Example main function for sending and modifying bookings
    """
    exec_date = sys.argv[1]
    env = sys.argv[2]

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.WARNING)

    eulerian = client(env)

    send_new_bookings(eulerian, exec_date, env)

    modify_booking(eulerian, 1901685652, 350.19)


if __name__ == "__main__":
    main()

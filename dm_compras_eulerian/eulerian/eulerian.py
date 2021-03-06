# -*- coding: utf-8 -*-

"""
eulerian
~~~~~~~~~~~~~~~~
This module provides a Eulerian object to make requests to a subset
of the Eulerian api.
"""

import json
from urllib.parse import urlencode
import logging

import requests


def check_for_errors(func):
    """Decorator to check API response"""
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        if response.status_code == 200:
            response_text = json.loads(response.text)
            if response_text['error']:
                logging.error(response_text['error_msg'])
        else:
            logging.error('%s - %s', response.url, response.reason)
        return response

    return wrapper


class Eulerian:
    """A Eulerian API wrapper."""

    def __init__(self, auth_token, customer, site, datacenter='com'):
        self.auth_token = auth_token
        self.customer = customer
        self.datacenter = datacenter
        self.site = site
        self.domain_api = f'{customer}.api.eulerian.{datacenter}/ea/v2'
        self.url_api = f'https://{self.domain_api}/{self.auth_token}/ea/{self.site}/report/order'

    @check_for_errors
    def replay(self, order_ref, real_amount, ereplay_time, **kwargs):
        """Ingest"""
        url = f'{self.url_api}/replay.json'
        params = {'ref': order_ref,
                  'amount': real_amount,
                  'ereplay-time': ereplay_time,
                  **kwargs
                  }
        response = requests.get(url, params=urlencode(params))
        return response

    @check_for_errors
    def valid(self, order_ref, order_newamount=None, ea_apiasync=None):
        """Modify"""
        url = f'{self.url_api}/valid.json'
        params = {'order-ref': order_ref}
        if order_newamount:
            params['order-newamount'] = order_newamount
        if ea_apiasync:
            params['ea-apiasync'] = ea_apiasync
        response = requests.get(url, params=urlencode(params))
        return response

    @check_for_errors
    def cancel(self, order_ref):
        """Cancel"""
        url = f'{self.url_api}/cancel.json'
        params = {'order-ref': order_ref}
        response = requests.get(url, params=urlencode(params))
        return response

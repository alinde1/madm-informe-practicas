"""
A simple wrapper to Adform API. Only the needed operations are implemented.
Refer to https://api.adform.com/help/ for detailed info.
"""

from __future__ import print_function
import json
import os
import errno

import requests

try:
    import unicodecsv as csv
except ImportError:
    import csv


class Adform(object):
    """
    Adform
    """
    base_dir = "/mnt/tmp"
    _token = None
    client_id = "client@clients.adform.com"
    client_secret = "**************************************"
    _api_url = "https://api.adform.com/v1"

    _ticket = None
    _user = "user"
    _password = "************************"
    _client_setup = "******************************************"

    def __init__(self):
        pass

    @property
    def access_token(self):
        """
        Get OAuth token. This token will be used in all API calls.
        It's saved for sharing among requests
        """
        if Adform._token is None:
            params = {
                'client_id': Adform.client_id,
                'client_secret': Adform.client_secret,
                'grant_type': "client_credentials",
                'scope': "https://api.adform.com/scope/buyer.stats"
            }

            headers = {
                "content-type": "application/x-www-form-urlencoded"
            }

            path = 'https://id.adform.com/sts/connect/token'
            response = requests.post(url=path, data=params, headers=headers)

            if response.status_code == 200:
                Adform._token = response.json()['access_token']
            else:
                raise ValueError('Ticket response code not 200')

        return Adform._token

    @property
    def ticket(self):
        """
        Authenticate with user/pass and get a ticket
        """
        if Adform._ticket is None:
            url = "https://api.adform.com/Services/Security/Login"
            user = Adform._user
            password = Adform._password
            response = requests.post(url, data=json.dumps({'Username': user, 'Password': password}))
            if response.status_code == 200:
                Adform._ticket = response.json()['Ticket']
            else:
                raise ValueError('Ticket response code not 200')

        return Adform._ticket

    @property
    def _dimensions(self):
        """ Get a list of available dimensions """
        url = '{}/reportingstats/agency/metadata/dimensions'.format(Adform._api_url)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.access_token)
        }
        payload = {"dimensions": []}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        return response.json()

    @property
    def _metrics(self):
        """ Get a list of available metrics """
        url = '{}/reportingstats/agency/metadata/metrics'.format(Adform._api_url)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.access_token)
        }
        payload = {"metrics": []}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        return response.json()

    def create_data_operation(self, payload):
        """ Create a data job """
        url = '{}/buyer/stats/data'.format(Adform._api_url)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.access_token)
        }
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code != 202:
            print(response.status_code)
            raise Exception
        location = response.headers['location']
        operation_id = location.split('/')[-1]
        return operation_id

    def get_operation_status(self, operation_id=None):
        """
        Retrieve the status of an operation given it's id.
        If no id is provided, returns a list of all operations.
        """
        headers = {
            "Authorization": "Bearer {}".format(self.access_token)
        }

        if operation_id is None:
            url = '{}/buyer/stats/operations'.format(Adform._api_url)
            response = requests.get(url, headers=headers)
            return response.json()

        url = '{}/buyer/stats/operations/{}'.format(Adform._api_url, operation_id)
        response = requests.get(url, headers=headers)
        status = response.json()['status']
        return status

    def get_data(self, operation_id, filename=None):
        """ Get data results """
        url = '{}/buyer/stats/data/{}'.format(Adform._api_url, operation_id)
        headers = {
            "Authorization": "Bearer {}".format(self.access_token)
        }
        response = requests.get(url, headers=headers)
        data = response.json()

        if filename:
            columns = data['reportData']['columnHeaders']
            rows = data['reportData']['rows']

            with open(filename, mode="w") as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(columns)
                for row in rows:
                    csv_writer.writerow(row)

        return data

    def get_file_list(self):
        """
        Get a list of available files in Adform
        """
        url = "{}/buyer/masterdata/files/{}".format(Adform._api_url, Adform._client_setup)
        cookie = {'authTicket': self.ticket}
        response = requests.get(url, cookies=cookie)
        return response.json()

    @staticmethod
    def full_path(base_dir, file_name, file_folder):
        """
        Returns the full path. Creates the directory if doesn't exists.
        """
        local_filename = "{}/{}/{}".format(base_dir, file_folder, file_name)
        if not os.path.exists(os.path.dirname(local_filename)):
            try:
                os.makedirs(os.path.dirname(local_filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        return local_filename

    def download_file(self, file_name, file_folder, base_dir=None):
        """
        Download file
        """
        if base_dir:
            _base_dir = base_dir
        else:
            _base_dir = Adform.base_dir

        cookie = {'authTicket': self.ticket}
        url = "{}/buyer/masterdata/download/{}/{}".format(Adform._api_url,
                                                          Adform._client_setup,
                                                          file_name)
        response = requests.get(url, cookies=cookie, stream=True)

        local_filename = self.full_path(_base_dir, file_name, file_folder)
        with open(local_filename, 'wb') as data_file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    data_file.write(chunk)

        return local_filename
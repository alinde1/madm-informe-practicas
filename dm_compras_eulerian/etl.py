"""

"""

from eulerian import Eulerian
import re
import boto3
import gzip
import sys
from io import BytesIO
import json
import datetime
import dateutil.parser


def main():
    e = Eulerian(auth_token='********************************',
                 customer='***',
                 site='************')
    exec_date = sys.argv[1]
    env = sys.argv[2]

    yyyy, mm, dd, hh24 = re.split('[-|_]', exec_date)
    path = 'CRM/Salesforce/Reservas/reservas{yyyy}/{mm}/{dd}/{hh24}'.format(**locals())

    files = ['aws-dev-dl-stream-reservas-1-2019-10-14-12-26-28-84c766fb-9a74-46de-9387-f9111db787df.gz']

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('bucket-cdr-landing-{}'.format(env))

    for file in files:
        key = '{}/{}'.format(path, file)
        obj = bucket.Object(key)
        body = obj.get()['Body'].read()
        gzipfile = BytesIO(body)
        gzipfile = gzip.GzipFile(fileobj=gzipfile)
        content = gzipfile.read()
        my_json = content.decode()
        reservas = [json.loads(data) for data in my_json.split('\n') if data]
        for reserva in reservas:
            ref = reserva['locator']
            amount = reserva['euroNetAmount']
            ereplay_time = dateutil.parser.parse(reserva['createdDate']).strftime('%s')
            print(ref, amount, ereplay_time)
            resp = e.replay(ref, amount, ereplay_time)
            if resp.status_code == 200:
                print(resp.url)
                print(resp.text)
            else:
                print('Error: {}'.format(resp.url))

    # resp = e.valid(1901685652, order_newamount=350.19)  # 330.91
    # if resp.status_code == 200:
    #     print(resp.url)
    #     print(resp.text)


if __name__ == "__main__":
    main()

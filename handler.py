import sys, os, json

here = os.path.dirname(os.path.realpath(__file__))
#sys.path.append(os.path.join(here, "./vendored"))
env_path = os.path.join(here, "./venv/lib/python2.7/site-packages/")
sys.path.append(env_path)


import requests
from contextlib import closing
from itertools import izip
from collections import OrderedDict
import csv
import re

not_found = {
    "statusCode": 404,
    "headers": {
        "Access-Control-Allow-Origin": "*",
    }
}

def dataset_preview(event, context):
    #url = "http://seshat.datasd.org.s3.amazonaws.com/special_events/special_events_list_datasd.csv"
    try:
        url = event['queryStringParameters']['datasetUrl']
        url_split = url.split('.')
        if url_split[-1] != 'csv':
            raise ValueError("Not a CSV")
    except:
        return not_found

    num_rows = 11
    try:
        num_rows = int(event['queryStringParameters']['numRows']) + 1
    except:
        pass

    rows = []
    counter = 0
    cols = []
    print num_rows

    with closing(requests.get(url, stream=True)) as r:
        reader = csv.reader(r.iter_lines(), delimiter=',', quotechar='"')
        for row in reader:
            if counter == 0:
                cols = row
            elif counter == num_rows:
                break
            else:
                i = iter(cols)
                row_dict = OrderedDict(izip(i, row))
                rows.append(row_dict)

            counter = counter + 1

    resp_body = rows

    # Access-Control for CORS
    # Content-Type for resp type
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json; charset=utf-8"
        },
        "body": json.dumps(resp_body)
    }

    return response

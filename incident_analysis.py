import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
import requests
import json
#import geoplotlib
import csv
import math
incidents = pd.read_csv('incidents_2015-2018.csv')

def geoCode(filename, output):
    '''
    gives latitude and longitude based on filename
    @param filename: the filename to output to and extract data from
    @param output: the file to output lat lng data to
    @return: None
    ''' 
    num_requests = 100
    inc = pd.read_csv(filename)
    with open('api_keys.json') as f:
        api_keys = json.load(f)
    params = {
        'key': api_keys['consumer_key'],
        'boundingBox': '40.544722, -88.844127, 39.397204, -87.524243' # Square box around CU
    }
    for batch in range(math.ceil(len(inc) / num_requests)):
        print("Batch #" + str(batch))
        location = []
        for i in range(num_requests):
            try:
                location.append(inc['Mapping Address'][i + batch * num_requests])
            except KeyError: # Used in last batch if not perfectly divisible by 100
                break
        params['location'] = location
        r = requests.get('http://mapquestapi.com/geocoding/v1/batch', params=params)
        if r.status_code == 200:
            results = [r.json()['results'][i]['locations'][0]['latLng'] for i in range(len(r.json()['results']))]
            csv_write(output, results, batch * num_requests + 1)
        else:
            print("Error: " + str(r.status_code))


def csv_write(filename, data, index):
    '''
    Writes to filename using headers from list in dicts.
    @param filename: the filename to write to
    @param data: the data to write to the rows
    @param index: the index to write data passed in
    @return: None
    '''
    with open(filename, mode='a') as crime_file:
        fieldnames = [key for key in data[0].keys()]
        crime_writer = csv.DictWriter(crime_file, fieldnames)
        if index == 1: # Writes header the initial time
            crime_writer.writeheader()
        print("Length: " + str(len(data)))
        for point in data:
            crime_writer.writerow(dict(point))

def clean_locations(filename, writeto):
    '''
    Removes rows that have nothing as the address.
    @param filename: the file with addresses to write to 
    @param writeto: the file to copy the cleaned data to
    @return: None
    '''
    df = pd.read_csv(filename)
    for i in range(len(list(df))):
        if list(df)[i] == 'Mapping Address': # Might need to change depending on header name
            location_index = i
    with open(writeto, 'w') as writefile:
        writer = csv.writer(writefile)
        writer.writerow(list(df))
        for i, row in enumerate(df.itertuples(False)):
            if type(row[location_index]) is not float: # Filters out nan, which is of type float
                writer.writerow(row)

def plot_points(dataset):
    '''
    Eventually will display locations on a geoplotlib map.
    @param dataset: the csv file to read
    @return: None
    '''
    crimes = geoplotlib.utils.read_json(dataset)
    geoplotlib.dot(crimes)
    geoplotlib.show()
clean_locations('incidents_2015-2018.csv', 'clean_incidents.csv')
geoCode('clean_incidents.csv', 'inc_latlng.csv')
#plotPoints(geoCode())


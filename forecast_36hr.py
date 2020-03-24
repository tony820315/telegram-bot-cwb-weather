#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import logging
import calendar
import datetime
import configparser

import requests
import sqlite3


config = configparser.ConfigParser()
config.read('config.ini')
CWB_URL = config['CWB']['CWB_URL']
CWB_DB_PATH = config['CWB']['CWB_DB_PATH']
logging.basicConfig(level=logging.DEBUG)

def get_data_from_cwb(data_id, auth_key, params={}):
    '''limit, offset, format, locationName, elementName, sort'''
    logging.info('getting data from CWB...')
    dest_url = CWB_URL + '{}'.format(data_id)
    r = requests.get(dest_url, headers={'Authorization': auth_key})
    params_list = ['{}={}'.format(key, params[key]) for key in params]
    params_str = '?' + '&'.join(params_list)
    dest_url += params_str
    logging.debug('dest_url: {}'.format(dest_url))
    
    if r.status_code != 200:
        logging.error('r.status_code: {}'.format(r.status_code))
        return None

    data = r.json()
    
    if data.get('success') != 'true':
        return None
    return data

def parse_json_to_dict_city(data):
    logging.info('parsing {} ...'.format(data['records']['datasetDescription'].encode('utf-8')))
    output = {}
    locations = data['records']['location']
    for l in locations:
        location_name = l['locationName']
        output[location_name] = {}
        factors = l['weatherElement']
        for f in factors:
            factor_name = f['elementName']
            periods = f['time']
            for p in periods:
                #  Taiwan GMT+8
                end_time_ts = calendar.timegm(datetime.datetime.strptime(p['endTime'], '%Y-%m-%d %H:%M:%S').timetuple()) - 8 * 3600
                time_key = str(end_time_ts)
                if time_key not in output[location_name]:
                    output[location_name][time_key] = {}
                forecast_status = p['parameter']['parameterName']
                if factor_name == 'Wx':
                    forecast_value = p['parameter']['parameterValue']
                    output[location_name][time_key][factor_name] = int(forecast_value)
                elif factor_name in ['MaxT', 'MinT', 'PoP']:
                    forecast_status = int(forecast_status)
                    output[location_name][time_key][factor_name] = forecast_status
                elif factor_name == 'CI':
                    output[location_name][time_key][factor_name] = forecast_status
    return output
            
def dump_dict_to_json_file(dict_data, filename):
    logging.info('dump to json file...')
    with open(filename, 'w') as fp:
        json.dump(dict_data, fp, ensure_ascii=False)

def read_json_file(filename):
    with open(filename, 'r') as reader:
        jf = json.loads(reader.read())
    return jf

def create_table_city():
    conn = sqlite3.connect(CWB_DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS CWB \
    (EndTime real, Location text, Wx int, MaxT int, MinT int, PoP int, CI text, PRIMARY KEY (EndTime, Location))''')
    conn.commit()
    conn.close()

def insert_data_city(dict_data):
    conn = sqlite3.connect(CWB_DB_PATH)
    c = conn.cursor()
    for loc in dict_data:
        for time in dict_data[loc]:
            Wx = int(dict_data[loc][time]['Wx'])
            MaxT = int(dict_data[loc][time]['MaxT'])
            MinT = int(dict_data[loc][time]['MinT'])
            PoP = int(dict_data[loc][time]['PoP'])
            CI = str(dict_data[loc][time]['CI'])
            # print(loc, time, Wx, MaxT, MinT, PoP, CI)
            c.execute('''INSERT OR REPLACE INTO CWB VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                                (int(time), str(loc), Wx, MaxT, MinT, PoP, str(CI)))
            conn.commit()
    conn.close()
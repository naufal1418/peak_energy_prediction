import requests
import datetime
import time
import json
import csv
import os

# open csv file 
# write file for one row every time
def csv_write(c_path,c_data):
    with open(c_path, mode="a", encoding="utf-8", newline="",errors='ignore') as f:
        csv.writer(f).writerow(c_data)


# request for the data from the URL
def request_get(c_url):
    response = requests.get(c_url,verify=True,allow_redirects=True)
    response_result = response.content.decode("utf-8")
    return response_result


# function for get data
# input date and return data
def get_observations(c_time):
    res_text = request_get(f'https://api.weather.com/v1/location/YPPH:9:AU/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&startDate={c_time}')
    res_json = json.loads(res_text)
    return res_json['observations']

# convert time into local time
def convert_time(c_wich_time):
    otherStyleTime = time.strftime("%H:%M:%S", time.localtime(c_wich_time))
    return str(otherStyleTime)

# give a range of date
def date_range(begin, end):
    datlst = []
    while begin <= end:
        datlst.append(begin)
        begin_convert = datetime.datetime.strptime(begin, "%Y%m%d")
        days1_timedelta = datetime.timedelta(days=1)
        begin = (begin_convert + days1_timedelta).strftime("%Y%m%d")
    return datlst




if __name__ in "__main__":

    for i in date_range('20060101','20060102'):  # Changing time range here
        data_time = i 

        save_path = f'{data_time}.csv'

        if(os.path.exists(save_path)):
            os.remove(save_path)
        csv_write(save_path,['Time','Temperature','Dew Point','Humidity','Wind','Wind Speed','Wind Gust','Pressure','Precip.','Condition'])

        all_data_list = get_observations(data_time)
        for item in all_data_list:
            info_time = item['valid_time_gmt'] # Time
            info_temp = f"{item['temp']} °F" # Temperature
            info_dewpt = f"{item['dewPt']} °F" # Dew Point
            info_rh =  f"{item['rh']} %" # Humidity
            info_wdir = item['wdir_cardinal'] # Wind
            info_wspd =  f"{item['wspd']} mph"  # Wind Speed
            info_gust = '0 mph' if item['gust'] == None else f"{item['gust']} mph" # Wind Gust
            info_pressure = f"{item['pressure']} in"  # Pressure
            info_precip_hrly = f"0.0 in" if item['precip_hrly'] == None else f"{item['precip_hrly']} in" # Precip.
            info_phrase = item['wx_phrase'] # Condition
            write_data = [convert_time(info_time),info_temp,info_dewpt,info_rh,info_wdir,info_wspd,info_gust,info_pressure,info_precip_hrly,info_phrase]
            csv_write(save_path,write_data)
            print(write_data)
        print('Done')

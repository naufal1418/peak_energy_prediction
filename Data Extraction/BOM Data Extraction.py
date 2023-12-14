from audioop import maxpp
import ftplib
import os
import xml.etree.ElementTree as ET
import pandas as pd
import csv
import numpy as np

def pull_forecast():
    with ftplib.FTP('ftp2.bom.gov.au', "anonymous", "guest") as ftp:
        ftp.cwd("anon/gen/")
        ftp.cwd('fwo')
        filenames = ftp.nlst()
        for filename in filenames:
            if filename == "IDW14199.xml":
                local_filename = os.path.join('Data/raw/BOM/7dayforecast/', filename)
                with open( local_filename, 'wb') as file :
                    ftp.retrbinary('RETR ' + filename, file.write)
                break
        ftp.quit()

def pull_historical():
    with ftplib.FTP('ftp2.bom.gov.au', 'anonymous', 'guest') as ftp:
        ftp.cwd("anon/gen/clim_data/IDCKWCDEA0/tables/wa")
        locs = ftp.nlst()
        for loc in locs:
            if loc == "daily.html":
                continue
            ftp.cwd("/anon/gen/clim_data/IDCKWCDEA0/tables/wa/"+loc)
            filenames = ftp.nlst()
            for filename in filenames:
                if not os.path.exists('Data/raw/BOM/HistoricalWeather/' + loc):
                    os.mkdir('Data/raw/BOM/HistoricalWeather/' + loc)
                local_filename = os.path.join('Data/raw/BOM/HistoricalWeather/', loc, filename)
                if os.path.exists(local_filename):
                    continue
                with open( local_filename, 'wb+') as file :
                    ftp.retrbinary('RETR ' + filename, file.write)
        ftp.quit()

def xml_forecast_to_frame():
    tree = ET.parse("Data/raw/BOM/7dayforecast/IDW14199.xml")
    root = tree.getroot()
    locs = root.findall(".//*[@type='location']")
    for loc in locs:
        dates = [child.attrib['start-time-local'] for child in loc][1:]
        location = loc.attrib["description"]
        min = [i.text for i in loc.findall(".//*[@type='air_temperature_minimum']")]
        max = [i.text for i in loc.findall(".//*[@type='air_temperature_maximum']")]
        rain = [i.text for i in loc.findall(".//*[@type='probability_of_precipitation']")][1:]
        precis = [i.text for i in loc.findall(".//*[@type='precis']")][1:]

        #print(dates, location, min, max, rain, precis)

        forecast_df = pd.DataFrame(data=np.array([min, max, rain, precis]).T, index=dates, columns=["min", "max", "rain", "precis"])

        local_filename = os.path.join('Data/raw/BOM/7dayforecast/', location)
        forecast_df.to_csv(local_filename)

def csv_historical_to_frame():
    dirs = os.listdir("Data/raw/BOM/HistoricalWeather")
    for dir in dirs:
        print(dir)
        if not os.path.isdir("Data/raw/BOM/HistoricalWeather/" + dir):
            continue
        full_df = pd.DataFrame(columns=["Station", "Date", "Transpiration", "Rain", "Evaporation", 
            "Max Temp", "Min Temp", "Max Hum", "Min Hum", "Av Wind", "Solar Rad"])
        files = os.listdir("Data/raw/BOM/HistoricalWeather/" + dir)
        for file in files:
            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join("Data/raw/BOM/HistoricalWeather/", dir, file), 
                    skiprows=12, skipfooter=1, encoding="WINDOWS-1252", engine="python", header=0,
                    names=["Station", "Date", "Transpiration", "Rain", "Evaporation", 
                        "Max Temp", "Min Temp", "Max Hum", "Min Hum", "Av Wind", "Solar Rad"])
                full_df = pd.concat([full_df, df], ignore_index=True)
        full_df["Date"] = pd.to_datetime(full_df["Date"], format="%d/%m/%Y")
        full_df.sort_values("Date", inplace=True, ignore_index=True)
        full_df.to_csv("Data/edited/HistoricalWeather/" + dir + ".csv")


#pull_forecast()
#xml_forecast_to_frame()

### We should never have to run these again (unless we want to train on a bit more data)
#pull_historical()
#csv_historical_to_frame()
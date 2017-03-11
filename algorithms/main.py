import pandas as pd
import numpy as np
import MySQLdb as db
import pymongo as pm
import datetime as dt
import urllib2, re, time, json

mcon = pm.MongoClient('10.129.23.41',27017)
mdb = mcon['data']
seil_col = mdb['temp_k_seil']

fl_name = 'out.%d.log' %int(time.time())
logger=  open(fl_name,'a+')

#get internal temperature
def latest_internal_temperature(lanes, zones):
    data_rows = []
    for lane in lanes:
        for zone in zones:
            query = 'select timestamp,lane,zone, temperature, from_unixtime(timestamp) from temperature where class=213 and lane=%d and zone=%d order by timestamp desc limit 1' %(lane, zone)
            con = db.connect('10.129.23.161','reader','datapool','datapool')
            cursor = con.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                data_rows.append(row)
    
    df = pd.DataFrame(data_rows, columns= ['epoch','lane','zone','temperature', 'timestamp'])
    return df

# get external temperature
def latest_external_temperature(lanes, zones):
    data_rows = []
    for lane in lanes:
        for zone in zones:
            query = 'select timestamp,lane,zone, temperature, from_unixtime(timestamp) from temperature where class=213 and lane=%d and zone=%d order by timestamp desc limit 1' %(lane, zone)
            con = db.connect('10.129.23.161','reader','datapool','datapool')
            cursor = con.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                data_rows.append(row)
    
    df = pd.DataFrame(data_rows, columns= ['epoch','lane','zone','temperature', 'timestamp'])
    return df

# get internal humidity
def latest_internal_humidity(nodes):
    latest_tmp_array = np.zeros((len(nodes),3))
    ts_list = []
    for node in nodes:
        docs = seil_col.find({"id":{"$eq":node}}).sort("TS",-1).limit(1)
        for doc in docs:
            latest_tmp_array[node-1][0] = doc['id']
            latest_tmp_array[node-1][1] = doc['TS']
            latest_tmp_array[node-1][2] = doc['humidity']
            ts_list.append(dt.datetime.fromtimestamp(doc['TS']).strftime('%Y/%m/%d %H:%M:%S'))

    df = pd.DataFrame(latest_tmp_array, columns=['id','epoch','humidity'])
    df['timestamp'] = pd.Series(ts_list)  
    
    return df

#get external humidity
def latest_external_humidity():
    now_ = dt.datetime.now()
    data_lines= []
    url = "http://api.openweathermap.org/data/2.5/weather?q=Powai,in&APPID=3158204b045ed3d8229ae52291d065f2"
    resp = urllib2.urlopen(url)
    data = resp.readlines()
    json_obj = json.loads(data[0])
    return json_obj['main']

# determine the part of the day
def part_of_day():
    now_ = dt.datetime.now()
    if now_.hour in [8,9,10]:
        return 1
    elif now_.hour in [11,12,13,14]:
        return 2
    elif now_.hour in [15,16]:
        return 3
    elif now_.hour in [17,18,19]:
        return 4
    else:
        return 5


while True:
    # internal temperature
    itmp_df = latest_internal_temperature([1,2,3],[1,2,3,4])

    # external temperature
    etmp_df = latest_external_temperature([1,2,3],[5])

    # internal humidity
    ihum_df = latest_internal_humidity([1,2,3,4])

    # external humidity
    ehum_df = latest_external_humidity();

    max_temp_rgn = itmp_df.loc[itmp_df['temperature'].idxmax()] # max temperature region
    min_temp_rgn =  itmp_df.loc[itmp_df['temperature'].idxmin()] # min temperature region
    avg_tmp = np.nanmean(itmp_df['temperature'])
    #base_temp = np.nanmean(etmp_df['temperature']) # the temperature that should exist without any intervention ...
    base_temp = 29.0
    rh_int = np.nanmean(list(ihum_df['humidity'])) # avg internal humidity
    rh_ext = ehum_df['humidity']# externl humidity

    zone = part_of_day() # defined in parts_of_day list
    #-------------------------------------------------------------------------------------------------------------------------#
    #print type(max_temp_rgn['temperature']), type(min_temp_rgn['temperature']), type(base_temp), type(avg_tmp), type(rh_int), type(rh_ext), zone
    out_str= '%s, %0.3f, %0.3f, %0.3f, %0.3f, %0.3f, %0.3f, %d' %(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), max_temp_rgn['temperature'], min_temp_rgn['temperature'], base_temp, avg_tmp, rh_int, rh_ext, zone)
    status_str = ''
    #-------------------------------------------------------------------------------------------------------------------------#

    if (max_temp_rgn['temperature'] - min_temp_rgn['temperature']) == 1.0:
        while abs(max_temp_rgn['temperature'] - min_temp_rgn['temperature']) <= 1.0:
            status_str = 'circulate  hot regions'
            break
    else:
        if rh_int > 50.0:
            if rh_ext > 50.0:
                if zone in  [1,2]:
                    if abs(avg_tmp - base_temp) < 2.0:
                        status_str = 'hvac and low circulation' # mechanical ventilation
                    elif abs(avg_tmp - base_temp) >= 2.0:
                        status_str = 'circulate 0.1' 
                elif zone in [3,4]:
                    if abs(avg_tmp - base_temp) < 2.0:
                        status_str = 'cycle hvacs 1' # maintain a certain temperature
                    elif abs(avg_tmp - base_temp) >= 2.0:
                        status_str = 'circulate 0.2'

            elif rh_ext >= 40.0 and rh_ext < 50.0:
                if zone in  [1,2]:
                    status_str = 'circulate 0.3'
                elif zone in [3,4]:
                    if abs(avg_tmp - base_temp) < 1.0:
                        status_str = 'circulate 0.4'
                    elif abs(avg_tmp - base_temp) >= 1.0:
                        status_str = 'ventilate 1' # natural ventilation

        elif rh_int >= 40.0 and rh_int < 50.0:
            if rh_ext > 50.0:
                if zone in  [1,2]:
                    if abs(avg_tmp - base_temp) < 2.0:
                        status_str = 'cycle hvacs 2'
                    elif abs(avg_tmp - base_temp) >= 2.0:
                        status_str = 'circulate 0.5' 
                elif zone in [3,4]:
                        status_str = 'circulate 0.6'

            elif rh_ext >= 40.0 and rh_ext < 50.0:
                if zone in  [1,2]:
                    status_str = 'circulate 0.7'
                elif zone in [3,4]:
                    if abs(avg_tmp - base_temp) < 1.0:
                        status_str = 'circulate 0.8'
                    elif abs(avg_tmp - base_temp) >= 1.0:
                        status_str = 'ventilate 2' # natural ventilation

    logger.write('%s, %s\n' %(out_str, status_str))
    logger.flush()
    time.sleep(300)

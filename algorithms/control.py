'''
TODO: 
remaining part of the day
cycle hvacs along with fans ON? motivation of cycling between ACs
no inclusion of ambient temperature? internal temperature < ambient temperature?

The following code controls climate for the given humidity and base temperature.
'''


import pandas as pd
import numpy as np
import MySQLdb as db
import pymongo as pm
import datetime as dt
import urllib2, re, time, json, sys
import paho.mqtt.client as mqtt

from itertools import cycle
hvacIterator = cycle(range(3,5))

mcon = pm.MongoClient('10.129.23.41',27017)
mdb = mcon['data']
seil_col = mdb['temp_k_seil']

fl_name = 'control.%s.log' %dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger=  open(fl_name,'a+')

control_str = '' # current control
prev_control = '' # previously taken decision
prev_hvac_id = -1 # last turned on hvac

# keep a tap on the status of each control
CYCLE_HVAC_STATUS = 0
HVAC_STATUS = 0
CIRCULATE_STATUS = 0
EXHAUST_STATUS = 0


#mqtt brokers
TEST_BROKER = '10.129.23.30'
PROD_BROKER = '10.129.23.41'

BROKER = PROD_BROKER

#get internal temperature
def latest_internal_temperature(lanes, zones):
    data_rows = []
    con = db.connect('10.129.23.161','reader','datapool','datapool')
    cursor = con.cursor()
    for lane in lanes:
        for zone in zones:
            query = 'select timestamp,lane,zone, temperature, from_unixtime(timestamp) from temperature where class=213 and lane=%d and zone=%d order by timestamp desc limit 1' %(lane, zone)
            
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                data_rows.append(row)
    
    df = pd.DataFrame(data_rows, columns= ['epoch','lane','zone','temperature', 'timestamp'])
    con.close()
    return df

# get external temperature
def latest_external_temperature(lanes, zones):
    data_rows = []
    con = db.connect('10.129.23.161','reader','datapool','datapool')
    cursor = con.cursor()
    for lane in lanes:
        for zone in zones:
            query = 'select timestamp,lane,zone, temperature, from_unixtime(timestamp) from temperature where class=213 and lane=%d and zone=%d order by timestamp desc limit 1' %(lane, zone)
            
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                data_rows.append(row)
    
    df = pd.DataFrame(data_rows, columns= ['epoch','lane','zone','temperature', 'timestamp'])
    con.close()
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

# perform the climate control
def perform_control(control):
    '''
        perform the control
        control: which appliances to be turned on/off
        set the status flags as to which control to turn on/off
    '''
    global prev_hvac_id, HVAC_STATUS, CYCLE_HVAC_STATUS, CIRCULATE_STATUS, EXHAUST_STATUS
    addtnl_params = dict() # to hold additional parameters to control

    if control == 'hvac':
        print ' %s  Turning on the HVACS at 24deg C setpoint' %dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        HVAC_STATUS=1
        CIRCULATE_STATUS =1
        CYCLE_HVAC_STATUS = 0
        EXHAUST_STATUS = 0

        addtnl_params['fans'] = [1,2,3,4,5,6]

    elif control == 'circulate':
        print '%s Circulating' % dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        HVAC_STATUS = 0
        CYCLE_HVAC_STATUS = 0
        EXHAUST_STATUS=0
        CIRCULATE_STATUS=1

        addtnl_params['fans'] = [1,2,3,4,5,6]

    elif control == 'ventilate':
        print '%s Turning on the exhaust fans'  %dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        HVAC_STATUS = 0
        CYCLE_HVAC_STATUS = 0
        EXHAUST_STATUS = 1
        CIRCULATE_STATUS = 1

        addtnl_params['fans'] = [1,2,3,4,5,6]

    else:
        parts = control.split(' ') 
        
        if len(parts) == 3:
            hvac_id = int(parts[2])
            if prev_hvac_id == -1:
                print '%s Turning on HVAC %d only'  %(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),hvac_id)
                prev_hvac_id = hvac_id
            else:
                print '%s Turning off HVAC %d and Turning ON HVAC %d' %(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), prev_hvac_id, hvac_id)
                prev_hvac_id = hvac_id
            addtnl_params['hvac_id'] = hvac_id
            if hvac_id == 3:
                addtnl_params['fans'] = [4,5,6]
            elif hvac_id ==4:
                addtnl_params['fans'] = [1,2,3]

            HVAC_STATUS = 0
            CYCLE_HVAC_STATUS = 1
            EXHAUST_STATUS = 0
            CIRCULATE_STATUS = 1
        else:
            return 

    # turn on/off appliances accordingly
    turn_on_off_appliance(HVAC_STATUS, CYCLE_HVAC_STATUS, EXHAUST_STATUS, CIRCULATE_STATUS, addtnl_params)

def turn_on_off_appliance(hvac, cycle_hvac, exhaust, circulate, kwargs=None):
    
    # create client to control fans
    client = mqtt.Client("Thermal_comfort_publisher")
    client.on_connect = on_connect
    client.connect(PROD_BROKER, 1883, 60)
    
    # create client to control acs and exhaust
    client_test = mqtt.Client("Thermal_comfort_publisher")
    client_test.on_connect = on_connect
    client_test.connect(TEST_BROKER, 1883, 60)

    print hvac, cycle_hvac, exhaust, circulate,
    
    # if any additional parameters are to be passed, like which AC, FANS to turn ON and which ones to turn off
    if kwargs is not None:
        if len(kwargs.keys()) == 2:
            print kwargs['hvac_id'], kwargs['fans']
        else:
            print kwargs['fans']

        if circulate:
            # turn on fans
            for f in kwargs['fans']:
                mqtt_msg = 'system,2%d1' %f
                client.publish('action/SEIL/Appliance_Test/0', mqtt_msg, 2) 
                time.sleep(2)
        else:
            # turn off fans
            for f in kwargs['fans']:
                mqtt_msg = 'system,2%d0' %f
                client.publish('action/SEIL/Appliance_Test/0', mqtt_msg, 2) 
                time.sleep(2)

        if hvac:
            # turn on hvacs
            for i in range(2):
                client_test.publish('control/LHC/ACTX/3','1',1)
                time.sleep(1)
            for i in range(2):
                client_test.publish('control/LHC/ACTX/4','1',1)
                time.sleep(1)
        else:
            # turn off hvacs
            for i in range(2):
                client_test.publish('control/LHC/ACTX/3','0',1)
                time.sleep(1)
            for i in range(2):
                client_test.publish('control/LHC/ACTX/4','0',1)
                time.sleep(1)


        if exhaust:
            # turn on exhaust to ventilate some air
            client_test.publish('CONTROL/LHC/RELAY/101','1 0',1)
            time.sleep(2)
        else:
            # turn off exhaust fans
            client_test.publish('CONTROL/LHC/RELAY/101','1 1',1)
            time.sleep(2)

        if cycle_hvac:
            # cycle between hvacs
            on_topic = 'control/LHC/ACTX/%d' % kwargs['hvac_id']
            if kwargs['hvac_id'] == 3:
                off_topic = 'control/LHC/ACTX/4'
            elif kwargs['hvac_id'] == 4:
                off_topic = 'control/LHC/ACTX/3'

            for i in range(2):
                client_test.publish(on_topic, '1', 1)
                time.sleep(1)
            for i in range(2):
                client_test.publish(off_topic, '0', 1)
                time.sleep(1)

            turn_off_fans_list = []
            for fan in [1,2,3,4,5,6]:
                if fan not in kwargs['fans']:
                    turn_off_fans_list.append(fan)

            for f in turn_off_fans_list:
                mqtt_msg = 'system,2%d0' %f
                client.publish('action/SEIL/Appliance_Test/0', mqtt_msg, 2) 
                time.sleep(2)

        #else:
        #    # turn off hvacs
        #    for i in range(2):
        #        client_test.publish('control/LHC/ACTX/3','0',1)
        #        time.sleep(1)
        #    for i in range(2):
        #        client_test.publish('control/LHC/ACTX/4','0',1)
        #        time.sleep(1)

    else:
        # turn off everything
        print 'hvacs off   '
        for i in range(2):
            client_test.publish('control/LHC/ACTX/3','0',1)
            time.sleep(1)

        for i in range(2):
            client_test.publish('control/LHC/ACTX/4','0',1)
            time.sleep(1)
        print 'exhaust off   '
        client_test.publish('CONTROL/LHC/RELAY/101','1 1',1)
        time.sleep(2)
        print 'fans off'
        for f in [1,2,3,4,5,6]:
            mqtt_msg = 'system,2%d0' %f
            client.publish('action/SEIL/Appliance_Test/0', mqtt_msg, 2) 
            time.sleep(2)

    # disconnect the mqtt clients
    client.disconnect()
    client_test.disconnect()

#----------------------------------------MQTT CONNECTION DETAILS--------------------------------------#
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_connect(client, userdata, flags, rc):
    print 'Disconnecting from broker'

    

#client.connect(PROD_BROKER, 1883, 60)

client_test = mqtt.Client("Thermal_comfort_publisher_2")
client_test.on_connect = on_connect
#client_test.connect(TEST_BROKER, 1883, 60)
#------------------------------------------------------------------------------#

# turn off all climate control 
turn_on_off_appliance(HVAC_STATUS, CYCLE_HVAC_STATUS, EXHAUST_STATUS, CIRCULATE_STATUS)

while True:

    try:
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

        # zone wise internal temperature
        zone12 = np.nanmean(itmp_df[( itmp_df['zone'] == 1 )|( itmp_df['zone'] == 2 )].temperature)
        zone23 = np.nanmean(itmp_df[( itmp_df['zone'] == 2 )|( itmp_df['zone'] == 3 )].temperature)
        zone34 = np.nanmean(itmp_df[( itmp_df['zone'] == 3 )|( itmp_df['zone'] == 4 )].temperature)
        avg_tmp = np.nanmean(itmp_df['temperature'])
        #base_temp = np.nanmean(etmp_df['temperature']) # the temperature that should exist without any intervention ...
        base_temp = 29.0
        
        #rh_int = np.nanmean(list(ihum_df['humidity'])) # avg internal humidity
        rh_int = np.mean([hum for hum in list(ihum_df['humidity'])   if hum > 0])
        rh_ext = ehum_df['humidity']# externl humidity

        zone = part_of_day() # defined in parts_of_day list
        #-------------------------------------------------------------------------------------------------------------------------#
        #print type(max_temp_rgn['temperature']), type(min_temp_rgn['temperature']), type(base_temp), type(avg_tmp), type(rh_int), type(rh_ext), zone
        out_str= '%s, %0.3f, %0.3f, %0.3f, %0.3f, %0.3f, %0.3f, %d' %(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), max_temp_rgn['temperature'], min_temp_rgn['temperature'], base_temp, avg_tmp, rh_int, rh_ext, zone)
        status_str = ''
        #-------------------------------------------------------------------------------------------------------------------------#

        if (max_temp_rgn['temperature'] - min_temp_rgn['temperature']) == 1.0:
            while abs(max_temp_rgn['temperature'] - min_temp_rgn['temperature']) <= 1.0:
                control_str = 'circulate  hot regions'
                break
        else:
            if rh_int > 50.0:
                if rh_ext > 50.0:
                    if zone in  [1,2]:
                        if abs(avg_tmp - base_temp) < 2.0:
                            control_str = 'hvac' # mechanical ventilation
                        
                        elif abs(avg_tmp - base_temp) >= 2.0:
                            control_str = 'circulate' 
                        elif  avg_tmp > base_temp:
                            control_str = 'hvac'
                        
                    elif zone in [3,4]:
                        if abs(avg_tmp - base_temp) < 2.0:
                            control_str = 'cycle hvacs %d' %hvacIterator.next() # maintain a certain temperature
                        
                        elif abs(avg_tmp - base_temp) >= 2.0:
                            control_str = 'circulate'
                        
                        elif  avg_tmp > base_temp:
                            control_str = 'hvac'
                    else:
                        control_str = 'circulate'  

                elif rh_ext >= 40.0 and rh_ext < 50.0:
                    if zone in  [1,2]:
                        control_str = 'circulate'
                        
                    elif zone in [3,4]:
                        if abs(avg_tmp - base_temp) < 2.0:
                            control_str = 'cycle hvacs %d' %hvacIterator.next() # maintain a certain temperature
                         
                        elif abs(avg_tmp - base_temp) >= 2.0:
                            control_str = 'circulate'
                    else:
                        control_str = 'circulate'

            elif rh_int >= 40.0 and rh_int < 50.0:
                if rh_ext > 50.0:
                    if zone in  [1,2]:
                        if abs(avg_tmp - base_temp) < 2.0:
                            control_str = 'cycle hvacs %d' % hvacIterator.next()
                         
                        elif abs(avg_tmp - base_temp) >= 2.0:
                            control_str = 'circulate' 
                         
                    elif zone in [3,4]:
                            if  avg_tmp > base_temp:
                                control_str = 'hvac'
                            else:
                                control_str = 'circulate'
                    else:
                        control_str = 'circulate'
                         

                elif rh_ext >= 40.0 and rh_ext < 50.0:
                    if zone in  [1,2]:
                        control_str = 'circulate'
                        
                    elif zone in [3,4]:
                        if abs(avg_tmp - base_temp) < 1.0:
                            control_str = 'circulate'
                            
                        elif abs(avg_tmp - base_temp) >= 1.0:
                            control_str = 'ventilate' # natural ventilation
                    else:
                        control_str = 'circulate'
                else:
                    control_str = 'circulate'
            else:
            	control_str = 'circulate'


        if prev_control == control_str:
            status_str = 'same action'
           
        else:
            status_str = control_str
            prev_control = control_str

            # perform the desired control
            perform_control(control_str)

        logger.write('%s, %s\n' %(out_str, status_str))
        logger.flush()
        time.sleep(300)

    except KeyboardInterrupt:
        print 'Turning off climate control appliances'
        turn_on_off_appliance(0,0,0,0)
        print 'Ending thermal control'
        sys.exit(1)

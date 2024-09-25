# coding:utf-8
import codecs
import json
import ssl
import paho.mqtt.client as mqtt
import time
import datetime
import MBus
import Mutiloop_PM
import schedule
import flowMeter
from datetime import datetime

def on_connect(client, userdata, flags, rc):
    global is_connected
    if rc == 0:
        is_connected = True
    else:
        is_connected = False

def on_disconnect(client, userdata, rc):
    global is_connected
    is_connected = False

def MQTT_Connect_sta():
    try:
        global client_sta
        client_sta = mqtt.Client('', True, None, mqtt.MQTTv31)
        client_sta.username_pw_set('infilink_ShangriLa2024TPE', 'wCGTd25n')
        client_sta.tls_set(cert_reqs=ssl.CERT_NONE)
        client_sta.connect('mqtt-device.fetiot3s1.fetnet.net', 8884 , 60)
        client_sta.loop_start()
        time.sleep(2)
        client_sta.on_connect
    except:
        print("error_connect_Sta")

def MQTT_Connect_pro():
    try:
        global client_pro
        client_pro = mqtt.Client('', True, None, mqtt.MQTTv31)
        client_pro.username_pw_set('infilink_ShangriLa2024TPE', '5WufQ879')
        client_pro.tls_set(cert_reqs=ssl.CERT_NONE)
        client_pro.connect('mqtt-device.fetiot3p1.fetnet.net', 8884 , 60)
        client_pro.loop_start()
        time.sleep(2)
        client_pro.on_connect
    except:
        print("error_connect_Pro")

def Connect_Mqtt_Pro():
    try:
        client_pro.connect('mqtt-device.fetiot3p1.fetnet.net', 8884 , 60)
        return 1;
    except:
        return 0;

def Connect_Mqtt_Sta():
    try:
        client_pro.connect('mqtt-device.fetiot3p1.fetnet.net', 8884 , 60)
        return 1;
    except:
        return 0;

def FET_Publish_Product(Meter_data,access_token,timestamp):
    try:
        
        mod_payload = [
            {"access_token":access_token,
            "app":"ShangriLa2024TPE",
            "type":"electricity_meter",
            "data":[
                {"timestemp":timestamp,
                "values":{
                    "voltage_r_s":Meter_data[1],
                    "voltage_s_t":Meter_data[2],
                    "voltage_t_r":Meter_data[3],
                    "voltage_line_avg":Meter_data[4],
                    "current_r":Meter_data[5],
                    "current_s":Meter_data[6],
                    "current_t":Meter_data[7],
                    "current_phase_avg":Meter_data[8],
                    "frequency":Meter_data[0],
                    "power": Meter_data[9],
                    "power_kvar":Meter_data[10],
                    "energy":Meter_data[14],
                    "immediate_demand":Meter_data[13],
                    "pf":Meter_data[12],
                    "alive":Meter_data[16],
                    "type":"三相三線"
                    }}]}
            ]
    
        data03 = client_pro.publish('/SHANGRILA2024TPE/v1/telemetry/infilink',json.dumps(mod_payload))
        time.sleep(2)
        
    except:
        pass

def FET_Publish_Station(Meter_data,access_token,timestamp):
    try:
        
        mod_payload = [
            {"access_token":access_token,
            "app":"ShangriLa2024TPE",
            "type":"electricity_meter",
            "data":[
                {"timestemp":timestamp,
                "values":{
                    "voltage_r_s":Meter_data[1],
                    "voltage_s_t":Meter_data[2],
                    "voltage_t_r":Meter_data[3],
                    "voltage_line_avg":Meter_data[4],
                    "current_r":Meter_data[5],
                    "current_s":Meter_data[6],
                    "current_t":Meter_data[7],
                    "current_phase_avg":Meter_data[8],
                    "frequency":Meter_data[0],
                    "power": Meter_data[9],
                    "power_kvar":Meter_data[10],
                    "energy":Meter_data[14],
                    "immediate_demand":Meter_data[13],
                    "pf":Meter_data[12],
                    "alive":Meter_data[16],
                    "type":"三相三線"
                    }}]}
            ] 
    
        data03 = client_sta.publish('/SHANGRILA2024TPE/v1/telemetry/infilink',json.dumps(mod_payload))
        time.sleep(2)
        #print ("Station= " + data03)
        #print ("Station= " + mod_payload)
        
    except:
        pass

def FlowMeter_Publish_Station(Meter_data,access_token,timestamp):
    try:
        
        mod_payload = [
            {"access_token":access_token,
            "app":"ShangriLa2024TPE",
            "type":"flow_meter",
            "data":[
                {"timestemp":timestamp,
                "values":{
                    "flowmeter_rt_volume_flow_rate":Meter_data[0],
                    "flowmeter_rt_energy_gjhr":Meter_data[1],
                    "flowmeter_rt_energy_rth":Meter_data[2],
                    "flowmeter_rt_flow_rate":Meter_data[3],
                    "flowmeter_total_volume_flow_rate":Meter_data[4],
                    "flowmeter_total_energy_gjhr":Meter_data[5],
                    "flowmeter_total_energy_rth":Meter_data[6],
                    "flowmeter_temperature_inlet":Meter_data[7],
                    "flowmeter_temperature_outlet":Meter_data[8],
                    "alive":1
                    }}]}
            ]
        
        data03 = client_sta.publish('/SHANGRILA2024TPE/v1/telemetry/infilink',json.dumps(mod_payload))
        time.sleep(2)
        #print ("Station= " + data03)
        #print ("Station= " + mod_payload)
    except:
        pass

def FlowMeter_Publish_Production(Meter_data,access_token,timestamp):
    try:
        
        mod_payload = [
            {"access_token":access_token,
            "app":"ShangriLa2024TPE",
            "type":"flow_meter",
            "data":[
                {"timestemp":timestamp,
                "values":{
                    "flowmeter_rt_volume_flow_rate":Meter_data[0],
                    "flowmeter_rt_energy_gjhr":Meter_data[1],
                    "flowmeter_rt_energy_rth":Meter_data[2],
                    "flowmeter_rt_flow_rate":Meter_data[3],
                    "flowmeter_total_volume_flow_rate":Meter_data[4],
                    "flowmeter_total_energy_gjhr":Meter_data[5],
                    "flowmeter_total_energy_rth":Meter_data[6],
                    "flowmeter_temperature_inlet":Meter_data[7],
                    "flowmeter_temperature_outlet":Meter_data[8],
                    "alive":1
                    }}]}
                ]   
    
        data03 = client_pro.publish('/SHANGRILA2024TPE/v1/telemetry/infilink',json.dumps(mod_payload))
        time.sleep(2)
        #print ("Production= " + data03)
        #print ("Production= " + mod_payload)
    except:
        pass

def do_job():
    
    try:
        now = datetime.now()
        aligned_minute = now.minute - (now.minute % 5)
        aligned_time = now.replace(minute=aligned_minute, second=0, microsecond=0)
        timestamp = int(time.mktime(aligned_time.timetuple()))
        MutiPowerMeter4 = Mutiloop_PM.Read_MutiPowerMeter('/dev/ttyS3',1,0)
        MutiPowerMeter5 = Mutiloop_PM.Read_MutiPowerMeter('/dev/ttyS3',1,1)
        MutiPowerMeter6 = Mutiloop_PM.Read_MutiPowerMeter('/dev/ttyS3',1,2)
        MutiPowerMeter7 = Mutiloop_PM.Read_MutiPowerMeter('/dev/ttyS3',1,3)
        print(MutiPowerMeter4)
        print(MutiPowerMeter5)

        con_mqtt = Connect_Mqtt_Pro()
        if con_mqtt == 1:
            FET_Publish_Product(MutiPowerMeter4,"9246b6a6a699481e962e67c591641f09",timestamp)
            print('FET_Publish')
            FET_Publish_Product(MutiPowerMeter5,"4abe1031ab2641c78e39f86fad3e161f",timestamp)
            FET_Publish_Product(MutiPowerMeter6,"6feb2296bf8b42229571c5f4567dd7a8",timestamp)
            FET_Publish_Product(MutiPowerMeter7,"9988be5321b94e2ca00397708980d4b4",timestamp)
        else:
            MQTT_Connect_pro()
            print('error')
            time.sleep(2)


        
        con_mqtt = Connect_Mqtt_Sta()
        if con_mqtt == 1:
            FET_Publish_Station(MutiPowerMeter4,"9246b6a6a699481e962e67c591641f09",timestamp)
            FET_Publish_Station(MutiPowerMeter5,"4abe1031ab2641c78e39f86fad3e161f",timestamp)
            FET_Publish_Station(MutiPowerMeter6,"6feb2296bf8b42229571c5f4567dd7a8",timestamp)
            FET_Publish_Station(MutiPowerMeter7,"9988be5321b94e2ca00397708980d4b4",timestamp)
        else:
            MQTT_Connect_sta()
            time.sleep(2)
    except:
        pass

MQTT_Connect_pro()
MQTT_Connect_sta()

schedule.every(5).minutes.do(do_job)
#schedule.every(10).seconds.do(do_job)

if __name__ == "__main__":
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except:
            pass
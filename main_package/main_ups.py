import os
import csv
import time
import datetime
from pysnmp.hlapi import *

# Configuration from provided info
UPS_DEVICES = [
    {
        'name': '10F_UPS',
        'ip': '172.21.2.13',
        'port': 161,
        'community': 'public',
        'snmp_version': 2,
    },
    {
        'name': '9F_UPS',
        'ip': '172.21.3.11',
        'port': 161,
        'community': 'public',
        'snmp_version': 2,
    },
    {
        'name': '8F_UPS',
        'ip': '172.21.4.10',
        'port': 161,
        'community': 'public',
        'snmp_version': 2,
    },
    {
        'name': '7F_UPS',
        'ip': '172.21.6.10',
        'port': 161,
        'community': 'public',
        'snmp_version': 2,
    },
    {
        'name': '3F_UPS',
        'ip': '172.21.5.14',
        'port': 161,
        'community': 'public',
        'snmp_version': 2,
    },
]

POLL_INTERVAL = 3600  # 1 hour
LOG_DIRECTORY = 'logs'
SNMP_TIMEOUT = 5
SNMP_RETRIES = 3
DEBUG = False

# OID mappings based on RFC 1628
OIDS = {
    'Vin': '1.3.6.1.2.1.33.1.3.3.1.3.1',    # Input voltage (RMS Volts)
    'Vout': '1.3.6.1.2.1.33.1.4.4.1.2.1',   # Output voltage (RMS Volts)
    'Vbat': '1.3.6.1.2.1.33.1.2.5.0',       # Battery voltage (0.1 Volt DC)
    'Fin': '1.3.6.1.2.1.33.1.3.3.1.2.1',    # Input frequency (0.1 Hertz)
    'Fout': '1.3.6.1.2.1.33.1.4.2.0',       # Output frequency (0.1 Hertz)
    'Load': '1.3.6.1.2.1.33.1.4.4.1.5.1',   # Percent load (%)
    'Temp': '1.3.6.1.2.1.33.1.2.7.0',       # Battery temperature (Celsius)
}

def get_snmp_value(ups, oid):
    iterator = getCmd(
        SnmpEngine(),
        CommunityData(ups['community'], mpModel=ups['snmp_version'] - 1),
        UdpTransportTarget((ups['ip'], ups['port']), timeout=SNMP_TIMEOUT, retries=SNMP_RETRIES),
        ContextData(),
        ObjectType(ObjectIdentity(oid))
    )
    errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
    
    if errorIndication:
        if DEBUG:
            print(f"SNMP error for {ups['name']}: {errorIndication}")
        return None
    elif errorStatus:
        if DEBUG:
            print(f"SNMP status error for {ups['name']}: {errorStatus.prettyPrint()}")
        return None
    else:
        return varBinds[0][1]

def poll_ups():
    now = datetime.datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%H:%M:%S')
    
    if not os.path.exists(LOG_DIRECTORY):
        os.makedirs(LOG_DIRECTORY)
    
    for ups in UPS_DEVICES:
        log_file = os.path.join(LOG_DIRECTORY, f"UPS_{ups['name']}_{date_str}.csv")
        
        # Check if file exists to determine if header is needed
        file_exists = os.path.exists(log_file)
        
        with open(log_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write header only if file doesn't exist
            if not file_exists:
                writer.writerow(['Date', 'Time', 'Vin', 'Vout', 'Vbat', 'Fin', 'Fout', 'Load', 'Temp', 'UPS_Name'])
            
            row = [date_str, time_str]
            
            vin = get_snmp_value(ups, OIDS['Vin'])
            vout = get_snmp_value(ups, OIDS['Vout'])
            vbat = get_snmp_value(ups, OIDS['Vbat'])
            fin = get_snmp_value(ups, OIDS['Fin'])
            fout = get_snmp_value(ups, OIDS['Fout'])
            load_val = get_snmp_value(ups, OIDS['Load'])
            temp = get_snmp_value(ups, OIDS['Temp'])
            
            # Process values with units
            row.append(float(vin) if vin is not None else 'N/A')  # Vin: direct RMS Volts
            row.append(float(vout) if vout is not None else 'N/A')  # Vout: direct RMS Volts
            row.append(float(vbat) / 10 if vbat is not None else 'N/A')  # Vbat: /10 for Volts
            row.append(float(fin) / 10 if fin is not None else 'N/A')  # Fin: /10 for Hz
            row.append(float(fout) / 10 if fout is not None else 'N/A')  # Fout: /10 for Hz
            row.append(float(load_val) if load_val is not None else 'N/A')  # Load: direct %
            row.append(float(temp) if temp is not None else 'N/A')  # Temp: direct C
            row.append(ups['name'])
            
            writer.writerow(row)
            
            if DEBUG:
                print(f"Logged data for {ups['name']}: {row}")

if __name__ == '__main__':
    while True:
        poll_ups()
        time.sleep(POLL_INTERVAL)
import sys
import h5py
import numpy as np
from datetime import datetime, timedelta
from process_accel_sheet import parseAccelSheet
from model import get_daq_id_by_daq,insert_sensor_data


def parseDate(d):
    format = '%Y-%m-%d_%H_%M_%S'
    return datetime.strptime(d, format)

def formatDate(d):
    format = '%Y-%m-%d %H:%M:%S'
    return datetime.strftime(d, format)

def parseStartDateTime(h5name):
    return parseDate(h5name.split('/')[-1].split('.')[0])

def generateTimestamp(start_time, second):
    days=(second/(60*60))/24
    hours=(second-days*60*60*24)/(60*60)
    minutes=(second-days*60*60*24-hours*60*60)/60
    seconds=(second-days*60*60*24-hours*60*60-minutes*60)
    timestamp = formatDate(start_time + timedelta(days=days,seconds=seconds,minutes=minutes,hours=hours))

    return timestamp

def h5DataKeys(fname,root_group_name):
    f = h5py.File(fname, 'r')
    dataset = f['/' + root_group_name]
    data_keys=dataset.keys()
    f.close()
    return data_keys

def parseDataKeyFromDaqKey(daq_key,data_keys):
    key=None
    for data_key in data_keys:
        if data_key.endswith(daq_key):
            key=data_key
            break
    return key

def daqKeysInFile(fname,root_group_name):
    data_keys=h5DataKeys(fname,root_group_name)
    daq_keys=map(lambda x: x[5:], data_keys)
    return daq_keys

def h5Data(fname,root_group_name,daq_key):
    f = h5py.File(fname, 'r')
    start_time = parseStartDateTime(fname)
    dataset = f['/' + root_group_name]
    data_keys=h5DataKeys(fname,root_group_name)
    key=parseDataKeyFromDaqKey(daq_key,data_keys)

    data = dataset.get(key)
    N, M = data.shape
    second = 0
    for x in range(N):
        for y in range(M):
            value=data[x][y]
            timestamp = generateTimestamp(start_time,second)
            second+=1
            yield (timestamp,value)
    f.close()

def h5ToCSV(fname,root_group_name,metadata,daq_key):
    out_file_name = fname.split('.')[0] + '_'+str(daq_key)+'.csv'
    out_file = open(out_file_name, 'w')
    metadata_colums = sorted(metadata.values()[0].keys())
    column_names = ['timestamp', 'value', 'daq'] + metadata_colums
    out_file.write(",".join(column_names) + "\n")
    key_metadata = metadata[daq_key]

    data = h5Data(fname,root_group_name,daq_key)
    for timestamp,value in data:
        row = [timestamp, value, daq_key]
        for col_name in metadata_colums:
            row.append(metadata[daq_key][col_name])
        row = list(map(str, row))
        out_file.write(",".join(row) + "\n")
    f.close()

def h5ToDatabaseForDaq(fname,root_group_name,daq_key):
    data=h5Data(fname,root_group_name,daq_key)
    daq_id=get_daq_id_by_daq(daq_key)
    if not daq_id:
        return None
    rows=[]
    for timestamp,value in data:
        rows.append((daq_id,timestamp,value))
    insert_sensor_data(rows)

if __name__ == '__main__':
    fname = sys.argv[1]
    root_group_name = 'Data'
    accel_sheet_name = sys.argv[2]
    daq_key = sys.argv[3]
    metadata = parseAccelSheet(accel_sheet_name)
    h5ToCSV(fname, root_group_name, metadata, daq_key)

# import process_h5_file as p
# fname='./h5data/2017-12-02_12_53_17.h5'
# root_group_name='Data'
# daq_keys=p.daqKeysInFile(fname,root_group_name)
# for key in daq_keys:
#     p.h5ToDatabaseForDaq(fname,root_group_name,key)
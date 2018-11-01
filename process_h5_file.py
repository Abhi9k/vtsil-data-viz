import sys
import h5py
import numpy as np
from datetime import datetime, timedelta
from process_accel_sheet import parseAccelSheet


def parseDate(d):
    format = '%Y-%m-%d_%H_%M_%S'
    return datetime.strptime(d, format)


def formatDate(d):
    format = '%Y-%m-%dT%H:%M:%S'
    return datetime.strftime(d, format)


def extractData(fname, root_group_name,metadata,daq_num):
    
    out_file_name = fname.split('.')[0] + '_'+str(daq_num)+'.csv'
    out_file = open(out_file_name, 'w')
    start_time = parseDate(fname.split('/')[-1].split('.')[0])
    metadata_colums = sorted(metadata.values()[0].keys())
    column_names = ['timestamp', 'value', 'daq'] + metadata_colums
    out_file.write(",".join(column_names) + "\n")

    f = h5py.File(fname, 'r')
    dataset = f['/' + root_group_name]
    daq_keys = dataset.keys()

    key=daq_keys[daq_num]
    daq_key = key[5:]
    data = dataset.get(key)
    N, M = data.shape
    # TODO: case when daq metadata is not present
    key_metadata = metadata[daq_key]
    second = 0
    for x in range(N):
        for y in range(M):
            value = data[x][y]
            days=(second/(60*60))/24
            hours=(second-days*60*60*24)/(60*60)
            minutes=(second-days*60*60*24-hours*60*60)/60
            seconds=(second-days*60*60*24-hours*60*60-minutes*60)

            timestamp = formatDate(start_time + timedelta(days=days,seconds=seconds,minutes=minutes,hours=hours))
            row = [timestamp, value, daq_key]
            for col_name in metadata_colums:
                row.append(metadata[daq_key][col_name])
            row = list(map(str, row))
            out_file.write(",".join(row) + "\n")
            second += 1
    f.close()


if __name__ == '__main__':
    fname = sys.argv[1]
    root_group_name = 'Data'
    accel_sheet_name = sys.argv[2]
    daq_num = int(sys.argv[3])
    metadata = parseAccelSheet(accel_sheet_name)
    extractData(fname, root_group_name, metadata, daq_num)


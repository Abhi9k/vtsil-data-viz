from scipy import signal
from scipy import integrate
from datetime import timedelta
from model import (get_sensor_data_in_time_range,insert_psd)
from utils import (SENSOR_DATE_TIME_FORMAT,parseDate,formatDate,generateTimestamp)

REFRESH_INTERVAL_SEC = 5
BATCH_FETCH_TIME_MIN = 1

def power_spectrum(ts_data,sampling_f=256.0,scaling='density', window='hann',window_size=256):
	window=signal.get_window(window,window_size)
	return signal.welch(ts_data,fs=sampling_f,scaling=scaling,window=window)

def generate_psd_rows(start_datetime, end_datetime):
	st=formatDate(start_datetime,SENSOR_DATE_TIME_FORMAT)
	et=formatDate(end_datetime,SENSOR_DATE_TIME_FORMAT)
	rows=get_sensor_data_in_time_range(st,et)
	daqwise_data={}
	for daq_id,val in rows:
		if daq_id not in daqwise_data:
			daqwise_data[daq_id]={'ts':[]}
		daqwise_data[daq_id]['ts'].append(val)

	psd_rows=[]
	for daq_id in daqwise_data:
		freqs,power=power_spectrum(daqwise_data[daq_id]['ts'])
		average_power=integrate.simps(power)
		freqs=",".join(map(lambda x:str(x),freqs))
		spectrum_val=",".join(map(lambda x: str(x),power))
		psd_rows.append((daq_id,st,average_power,freqs,spectrum_val))
	return psd_rows

def generate_psd_rows_for_batch(start_time, end_time):
	start_datetime=parseDate(start_time,SENSOR_DATE_TIME_FORMAT)
	end_datetime=parseDate(end_time,SENSOR_DATE_TIME_FORMAT)

	start=start_datetime
	while start<=end_datetime:
		end=start+timedelta(seconds=REFRESH_INTERVAL_SEC)
		psd_rows=generate_psd_rows(start,end)
		start=end
		yield psd_rows







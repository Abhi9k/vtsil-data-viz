import MySQLdb
from process_accel_sheet import parseAccelSheet

db=MySQLdb.connect(user='root',passwd="password",db='vtsil')

def insert_rows_sensor_metadata(cursor,rows):
	return cursor.executemany(
		"""INSERT INTO sensor_metadata (daq,bias_level,floor_number,orientation,sensitivity,serial,x,y,z)
			VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",rows
		)

def insert_rows_sensor_data(cursor,rows):
	return cursor.executemany(
		"""INSERT INTO sensor_data (daq_id,timestamp,value)
		   VALUES (%s,%s,%s)""", rows)

def parse_rows_from_accel_sheet(fname,has_header=True):
	resp=parseAccelSheet(fname)
	col_mapping_ordered=[
		('Bias Level','bias_level'),
		('Floor Number','floor_number'),
		('Orientation','orientation'),
		('Sensitivity','sensitivity'),
		('Serial','serial'),
		('X','x'),
		('Y','y'),
		('Z','z')
	]
	rows=[]
	num_cols=9
	for key,value in resp.items():
		row=[key]
		for n1,n2 in col_mapping_ordered:
			if value[n1]:
				row.append(value[n1])
		if len(row)==num_cols:
			rows.append(tuple(row))
	return rows

def insert_sensor_metadata(rows):
	cursor=db.cursor()
	resp = insert_rows_sensor_metadata(cursor,rows)
	cursor.close()
	db.commit()

	return resp

def get_daq_id_by_daq(daq):
	cursor=db.cursor()
	cursor.execute(
		"""SELECT id FROM sensor_metadata
		   WHERE daq=%s""",(daq,))
	resp=cursor.fetchone()
	resp=resp[0] if resp else resp
	cursor.close()

	return resp

def insert_sensor_data(rows):
	cursor=db.cursor()
	resp=insert_rows_sensor_data(cursor,rows)
	cursor.close()
	db.commit()
	return resp
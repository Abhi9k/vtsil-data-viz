import MySQLdb
from config import config
from process_accel_sheet import parseAccelSheet
from utils import SENSOR_DATE_TIME_FORMAT,parseDate

db=MySQLdb.connect(user=config['DATABASE_USER'],
    passwd=config['DATABASE_PASSWORD'],db=config['DATABASE_NAME'])

def insert_rows_psd(cursor, rows):
    return cursor.executemany(
        """INSERT INTO psd (daq_id, timestamp, average_power, freqs, power_spectrum)
           VALUES (%s,%s,%s,%s,%s)""", rows)

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

def get_psd_data_in_time_range(start_time, end_time):
    st=parseDate(start_time,SENSOR_DATE_TIME_FORMAT)
    et=parseDate(end_time,SENSOR_DATE_TIME_FORMAT)
    cursor=db.cursor()
    cursor.execute(
        """SELECT daq_id, average_power, freqs, power_spectrum FROM psd
           WHERE timestamp>=%s and timestamp<=%s order by timestamp""",(st,et))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def get_sensor_data_in_time_range(start_time,end_time):
    st=parseDate(start_time,SENSOR_DATE_TIME_FORMAT)
    et=parseDate(end_time,SENSOR_DATE_TIME_FORMAT)
    cursor=db.cursor()
    cursor.execute(
        """SELECT daq_id,value FROM sensor_data
           WHERE timestamp>=%s and timestamp<=%s order by timestamp""",(st,et))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def insert_sensor_data(rows):
    global db
    try:
      cursor = db.cursor()
      resp=insert_rows_sensor_data(cursor,rows)
      cursor.close()
      db.commit()
    except (AttributeError, MySQLdb.OperationalError):
      db=MySQLdb.connect(
        user=config['DATABASE_USER'],passwd=config['DATABASE_PASSWORD'],
        db=config['DATABASE_NAME'])
      cursor = db.cursor()
      resp=insert_rows_sensor_data(cursor,rows)
      cursor.close()
      db.commit()

    return resp

def insert_psd(rows):
    global db
    try:
      cursor = db.cursor()
      resp=insert_rows_psd(cursor,rows)
      cursor.close()
      db.commit()
    except (AttributeError, MySQLdb.OperationalError):
      db=MySQLdb.connect(
        user=config['DATABASE_USER'],passwd=config['DATABASE_PASSWORD'],
        db=config['DATABASE_NAME'])
      cursor = db.cursor()
      resp=insert_rows_psd(cursor,rows)
      cursor.close()
      db.commit()

    return resp
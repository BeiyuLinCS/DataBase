#!/usr/bin/env python2
"""
This program is written in python2. To run:

python2 example_data_script.py
"""

import datetime
import copy
import psycopg2
import sys
import glob
import os
import re

# This program uses psycopg2 to connect to smarthomedata.
# Documentation can be found here.
# http://initd.org/psycopg/docs/
###  python example_data_script.py > ./atmo4.dat


db_conn = psycopg2.connect(database="",
                           host="",
                           port="",
                           user="",
                           password="")


"""
This function converts a 'local' timestamp to the UTC value given the testbed.
"""
def get_utc_stamp(tbname, local_stamp):
    utc_stamp = local_stamp
    SQL = "SELECT timezone('utc', timestamp %s at time zone testbed.timezone) "
    SQL += "FROM testbed WHERE tbname=%s;"
    data = (local_stamp, tbname,)
    with db_conn:
        with db_conn.cursor() as cr:
            cr.execute(SQL, data)
            #print cr.query
            result = cr.fetchone()
            utc_stamp = result[0]
    return utc_stamp

"""
This function returns ALL events from a testbed during the provided stamp ranges.
"""
def get_all_testbed_data(tbname, start_stamp, end_stamp, foutpath):
    
    
    orig_stdout = sys.stdout
    f = open(foutpath, 'w')
    sys.stdout = f

    result = list()
    # index vals  0          1            2       3       4        5            6             7         8         9     10      11       12      13        14
    SQL = "SELECT stamp_utc, stamp_local, serial, target, message, sensor_type, package_type, category, event_id, uuid, stamp,  channel, tbname, timezone, by "
    SQL += "FROM all_events WHERE tbname=%s AND stamp BETWEEN %s AND %s;"
    data = (tbname, start_stamp, end_stamp,)
    with db_conn:
        with db_conn.cursor() as cr:
            cr.execute(SQL, data)
            #print cr.query
            for row in cr:
                event = dict()
                # we do a deepcopy of the datetime object as python likes to
                # pass around object by reference if we are not careful.
                event["stamp_utc"] = copy.deepcopy(row[0])
                event["stamp_local"] = copy.deepcopy(row[1])
                event["serial"] = row[2]
                event["target"] = row[3]
                event["message"] = row[4]
                event["sensor_type"] = row[5]
                event["package_type"] = row[6]
                event["category"] = row[7]
                event["event_id"] = row[8]
                event["uuid"] = row[9]
                event["stamp"] = row[10]
                event["channel"] = row[11]
                event["tbname"] = row[12]
                event["timezone"] = row[13]
                event["by"] = row[14]
                #result.append(copy.deepcopy(event))
                print_db_style_events([event], foutpath)

    sys.stdout = orig_stdout
    f.close()
    return result

def print_db_style_events(records, foutpath):


    for row in records:
        print row["event_id"]," | ",row["uuid"]," | ",row["serial"]," | ",row["stamp"]," | ",row["stamp_utc"]," | ",row["stamp_local"]," | ",row["message"]," | ",row["by"]," | ",
        print row["category"]," | ",row["target"]," | ",row["package_type"]," | ",row["sensor_type"]," | ",row["channel"]," | ",row["tbname"]," | ",row["timezone"]
    
    return


def test_bed_inform(test_bed_name, start_time, end_time, foutpath):
    testbedname = test_bed_name
    local_start_stamp = start_time
    local_end_stamp = end_time


    utc_start_stamp = get_utc_stamp(testbedname, local_start_stamp)
    utc_end_stamp = get_utc_stamp(testbedname, local_end_stamp)
    tb_data = get_all_testbed_data(testbedname, utc_start_stamp, utc_end_stamp, foutpath)



if __name__== "__main__":
    count = 0
    finpath = "/net/files/home/blin/cookinfo/rt/atmo10/data.al"
    # finpath = "/Volumes/Seagate Backup Plus Drive/IAQ_Minute_Data/Atmo8/data.al"
    
    #foutpath_init = "/net/files/home/blin/PopulationModelling/ExtractedFeatures/EFGapTimeLabel/"
    print "here"

    # for file in glob.glob(os.path.join(finpath, '*/data.al')):
    file0 = re.split(r'\/', finpath)
    tbname = file0[-2]

    # if tbname in already_extracted_raw_home_names:
    #     continue

    count += 1
    print "/n"
    #print "number, tbname", count, tbname
    
    f = open(finpath, 'rU')
    data = f.readlines()
    print ("len of file", len(data))
    first_line = data[0]
    last_line = data[-1]
    f.close()

    split_first_line = re.split(r" ", first_line)
    print "split_first_line", split_first_line, split_first_line[0]

    split_last_line = re.split(r" ", last_line)
    print "split_last_line", split_last_line, split_last_line[0]

    # 2015-06-19 23:58:26.168538 Kitchen OFF Other_Activity
    # 2015-06-19 23:59:07.033849 Kitchen ON Other_Activity
    # 2015-06-19 23:59:08.163515 Kitchen OFF Other_Activity

    start_stamp = split_first_line[0]
    end_stamp = split_last_line[0]
    foutpath =  "/net/files/home/blin/cookinfo/rt/atmo10/raw_no_labelled_data.al"

    test_bed_inform(tbname, start_stamp, end_stamp, foutpath)


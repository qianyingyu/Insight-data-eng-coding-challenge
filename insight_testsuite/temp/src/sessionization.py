# Author: Qianying Yu
# Purpose: Calculate the duration and number of documents requested by a user when the user visits, and output the information over a inactivity period
# Input: Stream log csv files and a txt file containing inactivity period
# Output: a txt file containing user visiting information

import csv
import numpy as np
import sys

def get_data(filename):
    '''

    :param filename: input log csv file
    :return: stream data
    consider the input data as time series stream data, use dictionary to aggregate the data stream for every second
    '''
    stream = {}
    read_cnt = 0
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            read_cnt += 1
            # skip the header
            if read_cnt == 1:
                continue
            ip = row[0]
            stream_time = np.datetime64(row[1] + ' ' + row[2])
            document = row[4] + ',' + row[5] + ',' + row[6]
            if stream_time not in stream:
                stream[stream_time] = {ip: set([document])}
            else:
                if ip not in stream[stream_time]:
                    stream[stream_time][ip] = set([document])
                else:
                    stream[stream_time][ip].add(document)

    # count different number of document every second a user requests
    for k,v in stream.items():
        for i, c in v.items():
            count = len(c)
            stream[k][i] = count

    return stream


def get_inactivity_period(filename):
    '''

    :param filename: input inactivity period txt file
    :return: inactiv period
    '''
    with open(filename, 'r') as coverage:
        seconds = int(coverage.read())
        inactiv_p = np.timedelta64(seconds, 's')
    return inactiv_p

def session_output(data, rt_record, inactiv_p, filename):
    '''

    :param data: input stream data
    :param rt_record: real time record covered at inactive period, should be updated with stream data every seconds
    :param inactiv_p: inactiv period
    :param filename: output session txt file
    :return: rt_recod, helps with dealing with multiple stream data or distributed work environment
    '''
    with open(filename, "w") as output_file:
        for k,v in data.items():
            for curr_ip in list(rt_record):
                if k - rt_record[curr_ip][1] > inactiv_p:
                    session = [curr_ip] + [str(rt_record[curr_ip][0]).replace('T', ' '), str(rt_record[curr_ip][1]).replace('T', ' ')] + rt_record[curr_ip][2:]
                    output_file.write(','.join(str(item) for item in session) + '\n')
                    del rt_record[curr_ip]
            for i, c in v.items():
                if i not in rt_record:
                    rt_record[i] = [k, k, 1, c]
                else:
                    rt_record[i][1] = k
                    rt_record[i][2] = int((k - rt_record[i][0])/np.timedelta64(1,'s') + 1)
                    rt_record[i][3] += c

        for k,v in rt_record.items():
            session = [k] + [str(v[0]).replace('T', ' '), str(v[1]).replace('T', ' ')] + v[2:]
            output_file.write(','.join(str(item) for item in session) + '\n')
    output_file.close()
    return rt_record


def main():
    input_path_log = sys.argv[1]
    input_path_txt = sys.argv[2]
    output_path = sys.argv[3]
    stream = get_data(input_path_log)
    inactiv_p = get_inactivity_period(input_path_txt)
    session_output(stream, {}, inactiv_p, output_path)

if __name__ == '__main__':
    main()

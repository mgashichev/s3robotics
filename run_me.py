#!/usr/bin/env python
# -*- coding: utf-8 -*-
import rosbag
import cv2
from cv_bridge import CvBridge
from datetime import datetime
from s3_lib import *
from prj_logging import *
import os
import sys
import re


def parse_and_upload(bag_file, s3_conn, bucket=dst_bucket, img_time_interval=None, topic=None):
    logging.info(f"PARSING STARTED: {bag_file}")

    if img_time_interval is None:
        img_time_interval = ['1900-01-01', '2999-01-01']
    if img_time_interval[0] is None:
        img_time_interval[0] = '1900-01-01'
    if img_time_interval[1] is None:
        img_time_interval[1] = '2999-01-01'

    logging.info(f"time interval: {img_time_interval}")

    bag = rosbag.Bag(bag_file)

    start_time = bag.get_start_time()
    start_time = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%d_%H-%M-%S')

    end_time = bag.get_end_time()
    end_time = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%d_%H-%M-%S')

    logging.info(f"collecting file info")
    selected_topics = []
    if topic is None:
        for k, v in bag.get_type_and_topic_info()[1].items():
            if 'Image' in v[0]:
                selected_topics.append(k)
                logging.info(f"Topic={k} : Type={v[0]} : Messages={v[1]}")
    else:
        selected_topics.append(topic)
    logging.info(f"selected topics: {selected_topics}")

    # missing file:
    # https://github.com/rospypi/simple/blob/master/cv_bridge/python/cv_bridge/boost/cv_bridge_boost.py
    bridge = CvBridge()

    out_dir = 'out'
    os.makedirs(out_dir, exist_ok=True)

    s3_path_list = set()
    i = 0
    for topic, msg, t in bag.read_messages(topics=selected_topics):
        img_time = "{}-{}".format(datetime.utcfromtimestamp(t.secs).strftime('%Y-%m-%d_%H-%M-%S'), t.nsecs)

        if not (img_time_interval[0] < img_time < img_time_interval[1]):
            continue
        if 'compressed' in topic:
            cv_img = bridge.compressed_imgmsg_to_cv2(msg, desired_encoding="passthrough")
        else:
            cv_img = bridge.imgmsg_to_cv2(msg, desired_encoding="passthrough")

        out_file = f"{out_dir}/{img_time}.png"
        cv2.imwrite(out_file, cv_img)

        topic = str(topic).replace('/', '_')
        data_type = 'CompressedImage' if 'compressed' in topic else 'Image'
        s3_path = f'{topic}/{data_type}/{start_time}_{end_time}/'
        if s3_path not in s3_path_list:
            s3_path_list.add(s3_path)
            s3_make_dir(s3_conn, bucket, s3_path)
        s3_upload_file(s3_conn, out_file, dst_bucket, s3_path)
        os.remove(out_file)

        # i += 1
        # if i >= 50:
        #     break
    logging.info(f"PARSING FINISHED: {bag_file}")


def main(topic, stime, etime):
    logging.info('STARTING JOB')
    s3_session = s3_connection(url, access_key, secret_key)
    s3_list = s3_get_src_files(s3_session)
    s3_delete_files(s3_session)

    for s3_file in s3_list:
        bag_file = s3_download_file(s3_session, src_bucket, s3_file)
        # bag_file = 'src/scenarion_id=1_subtask_id=1.bag'
        parse_and_upload(bag_file, s3_session, bucket=dst_bucket, img_time_interval=[stime, etime], topic=topic)

    s3_upload_file(s3_session, log_file, dst_bucket)
    logging.info('FINISH.')


if __name__ == "__main__":
    topic = None
    stime = None
    etime = None
    try:
        args_len = len(sys.argv)
        if args_len < 2:
            logging.error("not enough args")
            raise

        for i in range(1, args_len):
            match i:
                case 1: # input_dir
                    if not re.search('[^/]*/[^/]*', sys.argv[1]):
                        logging.error("input dir must be like bucket/dir format")
                        raise

                    global src_bucket
                    global src_prefix

                    # src_bucket, src_prefix = sys.argv[1].split('/')

                case 2: # topic
                    topic = sys.argv[i]

                case 3: # start_time
                    stime = sys.argv[i]

                case 4: # end_time
                    etime = sys.argv[i]

        main(topic, stime, etime)

    except Exception as e:
        print(e)


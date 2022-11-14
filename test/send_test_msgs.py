# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Methods to handle ASGS database activity

    Author: Phil Owen, RENCI.org
"""
import os
import sys
import time
import argparse
import pika


def queue_message(queue: str, message: str):
    """

    :param queue:
    :param message:
    :return:
    """

    # set up AMQP credentials and connect to asgs queue
    credentials = pika.PlainCredentials(os.environ.get("RABBITMQ_USER"), os.environ.get("RABBITMQ_PW"))
    parameters = pika.ConnectionParameters(os.environ.get("RABBITMQ_HOST"), 5672, '/', credentials, socket_timeout=2)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # channel.queue_declare(queue="asgs_queue")
    channel.queue_declare(queue=queue)

    # channel.basic_publish(exchange='',routing_key='asgs_queue',body=message)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    connection.close()


if __name__ == '__main__':
    # main entry point for the rule run.

    # create a command line parser
    parser = argparse.ArgumentParser(description='help', formatter_class=argparse.RawDescriptionHelpFormatter)

    # assign the expected input args
    parser.add_argument('-f', '--filename', help='Input file name that contains test queue data')
    parser.add_argument('-q', '--queue', help='the name of the queue that will be posted to.')
    parser.add_argument('-p', '--pause', help='the amount of time to pause (in seconds) between queue submissions.', default=2)

    # parse the command line
    args = parser.parse_args()

    # open messages file
    with open(args.filename, 'r', encoding='utf-8') as f:
        # submit each line in the file to the queue
        for line in f:
            # the data is in json/text format
            msg_obj = line

            # send the msg to the queue specified
            queue_message(args.queue, msg_obj)

            # something for the user
            print(msg_obj)

            # pause between each queue submission
            time.sleep(int(args.pause))

    # exit with pass/fail
    sys.exit(0)

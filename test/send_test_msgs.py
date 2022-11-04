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
import time
import pika


def queue_message(message):
    """

    :param message:
    :return:
    """

    # set up AMQP credentials and connect to asgs queue
    credentials = pika.PlainCredentials(os.environ.get("RABBITMQ_USER"), os.environ.get("RABBITMQ_PW"))
    parameters = pika.ConnectionParameters(os.environ.get("RABBITMQ_HOST"), 5672, '/', credentials, socket_timeout=2)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # channel.queue_declare(queue="asgs_queue")
    channel.queue_declare(queue="asgs_props")

    # channel.basic_publish(exchange='',routing_key='asgs_queue',body=message)
    channel.basic_publish(exchange='', routing_key='asgs_props', body=message)
    connection.close()


# with open('ASGS_status_msgs.json', 'r', encoding='utf-8') as f:
# with open('run_property_example1.json', 'r', encoding='utf-8') as f:
# with open('run_property_example2.json', 'r', encoding='utf-8') as f:
# with open('run_property_example3.json', 'r', encoding='utf-8') as f:

# open messages file
with open('run_property_example3.json', 'r', encoding='utf-8') as f:
    for line in f:
        msg_obj = line
        queue_message(msg_obj)
        print(msg_obj)
        time.sleep(2)

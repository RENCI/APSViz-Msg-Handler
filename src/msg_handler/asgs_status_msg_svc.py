# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Entrypoint for the ASGS status queue message listener/handler

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import os
import pika

from src.common.logger import LoggingUtil
from src.common.asgs_queue_callback import AsgsQueueCallback


def run():
    """
    Fires up the ASGS status queue message listener/handler

    :return:
    """
    # get the log level and directory from the environment.
    log_level, log_path = LoggingUtil.prep_for_logging()

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.APSViz-Msg-Handler.asgs_status_msg_svc", level=log_level, line_format='medium', log_file_path=log_path)

    logger.info("Initializing asgs_status_msg_svc handler.")

    try:
        # set up AMQP credentials and connect to asgs queue
        credentials = pika.PlainCredentials(os.environ.get("RABBITMQ_USER"),  os.environ.get("RABBITMQ_PW"))

        # set up the connection parameters
        connect_params = pika.ConnectionParameters(os.environ.get("RABBITMQ_HOST"), 5672, '/', credentials, socket_timeout=2)

        # get a connection to the queue
        connection = pika.BlockingConnection(connect_params)

        # create a new queue channel
        channel = connection.channel()

        # specify the queue that will be listened to
        channel.queue_declare(queue='asgs_queue')

        logger.info("asgs_status_msg_svc channel and queue declared.")

        # get an instance to the callback handler
        queue_callback_inst = AsgsQueueCallback(_logger=logger)

        # specify the queue callback handler
        channel.basic_consume('asgs_queue', queue_callback_inst.asgs_msg_callback, auto_ack=True)

        logger.info('asgs_status_msg_svc configured and waiting for messages...')

        # start the queue listener/handler
        channel.start_consuming()
    except Exception:
        logger.exception("FAILURE - Problems initializing asgs_status_msg_svc.")


if __name__ == "__main__":
    run()

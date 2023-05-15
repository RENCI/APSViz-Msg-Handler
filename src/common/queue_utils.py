# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Queue Utils - Various queue utilities common to this project's components.

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import os
import pika

from src.common.asgs_constants import AsgsConstants
from src.common.logger import LoggingUtil


class QueueUtils:
    """
    class that has common methods to interact with queues

    """

    def __init__(self, _queue_name: str, _logger=None):
        """
        init the queue utilities object

        :param: _queue_name
        :param _logger:
        """

        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("APSVIZ.Msg-Handler.QueueUtils", level=log_level, line_format='medium', log_file_path=log_path)

        # declare the ECFlow and HEC/RAS target params to asgs keys mapping dict
        self.msg_transform_params = {'suite.physical_location': ['physical_location', 'monitoring.rmqmessaging.locationname'],
                                     'suite.instance_name': ['instance_name', 'instancename'], 'suite.project_code': [], 'suite.uid': ['uid'],
                                     'suite.adcirc.gridname': ['ADCIRCgrid', 'adcirc.gridname'], 'time.currentdate': ['currentdate'],
                                     'time.currentcycle': ['currentcycle'], 'forcing.advisory': ['advisory'],
                                     'forcing.ensemblename': ['asgs.enstorm', 'enstorm'], 'forcing.metclass': [],
                                     'forcing.stormname': ['stormname', 'forcing.tropicalcyclone.stormname'],
                                     'forcing.waves': ['config.coupling.waves'], 'forcing.stormnumber': ['storm', 'stormnumber'],
                                     'output.downloadurl': ['downloadurl'], 'forcing.vortexmodel': ['forcing.tropicalcyclone.vortexmodel']}

        # save the queue name
        self.queue_name = _queue_name

        # save the relay enabled flag
        self.relay_enabled = os.environ.get('RELAY_ENABLED', 'False').lower() in ('true', '1', 't')

        # define and init the object used to handle ASGS constant conversions
        self.asgs_constants_inst = AsgsConstants(_logger=self.logger)

    def start_consuming(self, callback):
        """
        Creates and starts consuming queue messages

        :param callback:
        :return:
        """
        try:
            # create a new queue message handler
            channel: pika.adapters.blocking_connection.BlockingChannel = self.create_msg_listener()

            # check to see if we got a channel to the queue
            if not channel:
                self.logger.error("Error: Did not get a channel to queue %s.", self.queue_name)
            else:
                # specify the queue callback handler
                channel.basic_consume(self.queue_name, callback, auto_ack=True)

                # start the queue listener/handler
                channel.start_consuming()

                self.logger.info('%s listener configured and waiting for messages.', self.queue_name)
        except Exception:
            self.logger.exception("Error: Exception consuming queue %s.", self.queue_name)

    def create_msg_listener(self):
        """
        Creates a new queue message listener

        :return:
        """
        # init the return
        # noinspection PyTypeChecker
        channel: pika.adapters.blocking_connection.BlockingChannel = None

        try:
            # set up AMQP credentials and connect to asgs queue
            credentials: pika.PlainCredentials = pika.PlainCredentials(os.environ.get("RABBITMQ_USER"), os.environ.get("RABBITMQ_PW"))

            # set up the connection parameters
            connect_params: pika.ConnectionParameters = pika.ConnectionParameters(os.environ.get("RABBITMQ_HOST"), 5672, '/', credentials,
                                                                                  socket_timeout=2)

            # get a connection to the queue
            connection: pika.BlockingConnection = pika.BlockingConnection(connect_params)

            # create a new queue channel
            channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()

            # specify the queue that will be listened to
            channel.queue_declare(queue=self.queue_name)

            self.logger.info('%s channel configured on %s:5672.', self.queue_name, os.environ.get("RABBITMQ_HOST"))
        except Exception:
            self.logger.exception("Error: Exception on the creation of channel to %s.", self.queue_name)

        # return the queue channel
        return channel

    def relay_msg(self, body: bytes, force: bool = False) -> bool:
        """
        relays a received message to another queue. it expects the value directly from the queue.

        :param: body
        :return:
        """
        # init the return value
        ret_val: bool = True

        # if relay is enabled or being forced
        if self.relay_enabled or force:
            # init the connection
            connection = None

            try:
                # create credentials
                credentials = pika.PlainCredentials(os.environ.get("RELAY_RABBITMQ_USER"), os.environ.get("RELAY_RABBITMQ_PW"))

                # create connection parameters
                parameters = pika.ConnectionParameters(os.environ.get("RELAY_RABBITMQ_HOST"), 5672, '/', credentials, socket_timeout=2)

                # get a connection to the queue
                connection = pika.BlockingConnection(parameters)

                # get a channel to the consumer
                channel = connection.channel()

                # create the queue (is this needed?)
                channel.queue_declare(queue=self.queue_name)

                # push the message to the queue
                channel.basic_publish(exchange='', routing_key=self.queue_name, body=body)

            except Exception:
                self.logger.exception("Error: Exception relaying message to queue: %s.", self.queue_name)

                # set the return status to fail
                ret_val = False
            finally:
                # close the connection if it was created
                if connection is not None:
                    connection.close()

        # return pass/fail
        return ret_val

    def transform_msg_to_asgs_legacy(self, run_params: dict) -> dict:
        """
        Transforms a ECFlow message into an ASGS message

        :return:
        """
        # save the entire incoming dict into the return
        ret_val: dict = run_params.copy()

        # go through the params, search for a target, transform it into the ASGS equivalent
        for key, values in self.msg_transform_params.items():
            # find this in the run params
            if key in run_params:
                # grab the new keys from the transform asgs keys list
                for new_key in values:
                    # add the transform key with the run props data value to the dict
                    ret_val.update({new_key: run_params[key]})

        # return the new set of transformed params
        return ret_val

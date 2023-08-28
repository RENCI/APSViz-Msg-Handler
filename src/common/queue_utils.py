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
from enum import Enum

import pika
from src.common.logger import LoggingUtil


class ReformatType(int, Enum):
    """
    Enum class that defines the system that needs to be synchronized
    with a copy/move/remove operation
    """
    SENTENCE_CASE = 1
    UPPERCASE = 2
    LOWERCASE = 3
    INTEGER = 4
    FLOAT = 5
    STRING = 6
    MAKE_INT = 7


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
        self.msg_extend_params = {'suite.physical_location': ['physical_location', 'monitoring.rmqmessaging.locationname'],
                                  'suite.instance_name': ['instance_name', 'instancename'], 'suite.project_code': [], 'suite.uid': ['uid'],
                                  'suite.adcirc.gridname': ['ADCIRCgrid', 'adcirc.gridname'], 'time.currentdate': ['currentdate'],
                                  'time.currentcycle': ['currentcycle'], 'forcing.advisory': ['advisory'],
                                  'forcing.ensemblename': ['asgs.enstorm', 'enstorm'], 'forcing.metclass': [],
                                  'forcing.stormname': ['stormname', 'forcing.tropicalcyclone.stormname'], 'forcing.waves': ['config.coupling.waves'],
                                  'forcing.stormnumber': ['storm', 'stormnumber'], 'output.downloadurl': ['downloadurl'],
                                  'forcing.vortexmodel': ['forcing.tropicalcyclone.vortexmodel']}

        # declare param transformation selections
        self.msg_transform_params = {'storm': ReformatType.INTEGER, 'stormnumber': ReformatType.MAKE_INT,
                                     'forcing.stormname': ReformatType.UPPERCASE, 'stormname': ReformatType.UPPERCASE,
                                     'forcing.tropicalcyclone.stormname': ReformatType.UPPERCASE}
        # save the queue name
        self.queue_name = _queue_name

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
        relays a received message to another queue. It expects the value directly from the queue.

        :param: body
        :param: force
        :return:
        """
        # init the return value
        ret_val: bool = True

        # get the flag (file existence) that indicates we are force stopping all message relaying
        # this flag overrides all other message relaying flags (presumed to be temporary)
        override_relay: bool = os.path.exists(os.path.join(os.path.dirname(__file__), '../', '../', str('norelay')))

        # this will force no message relaying
        if not override_relay:
            # get the relay credentials
            relay_user: str = os.environ.get("RELAY_RABBITMQ_USER")
            relay_password: str = os.environ.get("RELAY_RABBITMQ_PW")
            relay_host: str = os.environ.get("RELAY_RABBITMQ_HOST")

            # if there is a place specified to relay a message to
            if relay_user and relay_password and relay_host:
                # get the relay enabled flag. if force=true is passed in it will override the environment variable
                relay_enabled: bool = os.environ.get('RELAY_ENABLED', 'False').lower() in ('true', '1', 't') or force

                # if relay is enabled or being forced
                if relay_enabled:
                    # init the connection
                    connection = None

                    try:
                        # create credentials
                        credentials: pika.PlainCredentials = pika.PlainCredentials(relay_user, relay_password)

                        # create connection parameters
                        parameters: pika.ConnectionParameters = pika.ConnectionParameters(relay_host, 5672, '/', credentials, socket_timeout=2)

                        # get a connection to the queue
                        connection = pika.BlockingConnection(parameters)

                        # get a channel to the consumer
                        channel: pika.adapters.blocking_connection.BlockingChannel = connection.channel()

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

    def extend_msg_to_asgs_legacy(self, run_params: dict) -> dict:
        """
        Extends params from a ECFlow message to include ASGS message legacy params

        :return:
        """
        # save the entire incoming dict into the return
        ret_val: dict = run_params.copy()

        # go through the params, search for a target, transform it into the ASGS equivalent
        for key, values in self.msg_extend_params.items():
            # find this in the run params
            if key in run_params:
                # grab the new keys from the transform asgs keys list
                for new_key in values:
                    # add the transform key with the run props data value to the dict
                    ret_val.update({new_key: run_params[key]})

        # return the new set of transformed params
        return ret_val

    def transform_msg_params(self, run_params: dict) -> dict:
        """
        uses a mapping to determine if a parameter should be reformatted into another format.

        :param run_params:
        :return:
        """
        # save the entire incoming dict into the return
        ret_val: dict = run_params.copy()

        # init the param value
        param: str = ''

        # go through the params, search for a target, transform it into the ASGS equivalent
        for key, value in self.msg_transform_params.items():
            # find this in the run params
            if key in run_params:
                try:
                    # grab the parameter value
                    param: str = run_params[key]

                    # make sure there is something to reformat. if not just skip it...
                    if param is not None and len(param) > 0:
                        # for any reformatting type
                        match value:
                            case ReformatType.INTEGER:
                                # try to make the conversion
                                if param.isdigit():
                                    # make the conversion
                                    ret_val.update({key: str(int(param))})
                            case ReformatType.FLOAT:
                                # try to make the conversion
                                if param.replace('.', '1').replace('e', '1').isdigit():
                                    # make the conversion
                                    ret_val.update({key: str(float(param))})
                            case ReformatType.UPPERCASE:
                                # make the conversion
                                ret_val.update({key: param.upper()})
                            case ReformatType.LOWERCASE:
                                # make the conversion
                                ret_val.update({key: param.lower()})
                            case ReformatType.SENTENCE_CASE:
                                # make the conversion
                                ret_val.update({key: param[:1].upper() + param[1:].lower()})
                            case ReformatType.STRING:
                                # make the conversion
                                ret_val.update({key: str(param)})
                            case ReformatType.MAKE_INT:
                                # strip off all non-numeric characters
                                val = "".join(n for n in param if n.isnumeric())

                                # check to see if a number is left. if so, pad it
                                if val.isnumeric():
                                    ret_val.update({key: val.rjust(2, '0')})
                                # else this is not a number
                                else:
                                    ret_val.update({key: 'NaN'})
                            case _:
                                self.logger.error("Invalid conversion type found.")
                except Exception:
                    # log the exception
                    self.logger.error('Error: Could not convert %s value %s into %s.', key, param, value)

        # return the new set of transformed params
        return ret_val

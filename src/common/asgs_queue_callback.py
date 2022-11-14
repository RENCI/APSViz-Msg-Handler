# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    callback methods to handle posts to the RabbitMQ

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import os
import json
import pika

from src.common.logger import LoggingUtil
from src.common.general_utils import GeneralUtils
from src.common.asgs_constants import AsgsConstants
from src.common.asgs_db import AsgsDb


class AsgsQueueCallback:
    """
    callback methods to handle posts to the RabbitMQ
    """

    def __init__(self, _queue_name, _logger=None):
        """
        define the queue message handler for ASGS messages

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
            self.logger = LoggingUtil.init_logging("APSVIZ.Archiver.ASGSQueueCallback", level=log_level, line_format='medium', log_file_path=log_path)

        # save the queue name
        self.queue_name = _queue_name

        self.logger.info("Initializing ASGSQueueCallback for queue %s", _queue_name)

        # define and init the object used to handle ASGS constant conversions
        self.asgs_constants_inst = AsgsConstants(_logger=self.logger)

        # define and init the object that will handle ASGS DB operations
        self.asgs_db_inst = AsgsDb(self.asgs_constants_inst, _logger=self.logger)

        # create the general utilities class
        self.general_utils = GeneralUtils(_logger)

        self.logger.info("ASGSQueueCallback initialization for queue %s complete.", _queue_name)

    def asgs_msg_callback(self, channel, method, properties, body):
        """
        main worker that operates on the incoming ASGS messages from the queue

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger.info("Received ASGS status msg. Body is %s bytes.", len(body))

        # load the message
        msg_obj = json.loads(body)

        # get the site id from the name in the message
        site_id = self.asgs_constants_inst.get_lu_id_from_msg(msg_obj, "physical_location", "site")

        # get the 3vent type if from the event name in the message
        event_type_id, event_name = self.asgs_constants_inst.get_lu_id_from_msg(msg_obj, "event_type", "event_type")

        # get the 3vent type if from the event name in the message
        state_id, state_name = self.asgs_constants_inst.get_lu_id_from_msg(msg_obj, "state", "state_type")

        # get the event advisory data
        advisory_id = msg_obj.get("advisory_number", "N/A") if (msg_obj.get("advisory_number", "N/A") != "") else "N/A"

        # did we get everything needed
        if site_id[0] >= 0 and event_type_id >= 0 and state_id >= 0 and advisory_id != 'N/A':
            # check to see if there are any instances for this site_id yet
            # this might happen if we start up this process in the middle of a model run
            instance_id = self.asgs_db_inst.get_existing_instance_id(site_id[0], msg_obj)

            # if this is a STRT event, create a new instance
            if instance_id < 0 or (event_name == "STRT" and state_name == "RUNN"):
                self.logger.debug("create_new_inst is True - creating new inst")

                # insert the record
                instance_id = self.asgs_db_inst.insert_instance(state_id, site_id[0], msg_obj)

            else:  # just update instance
                self.logger.debug("create_new_inst is False - updating inst")

                # update the instance
                self.asgs_db_inst.update_instance(state_id, site_id[0], instance_id, msg_obj)

            # check to see if there are any event groups for this site_id and inst yet
            # this might happen if we start up this process in the middle of a model run
            event_group_id = self.asgs_db_inst.get_existing_event_group_id(instance_id, advisory_id)

            # if this is the start of a group of Events, create a new event_group record
            # qualifying group initiation: event type = RSTR
            # STRT & HIND do not belong to any event group??
            # For now, it is required that every event belong to an event group, so I will add those as well.
            # create a new event group if none exist for this site & instance yet or if starting a new cycle

            # +++++++++++++++++++++++++ Figure out how to stop creating a second event group
            #   after creating first one, when very first RSTR comes for this instance+++++++++++++++++++

            if event_group_id < 0 or (event_name == "RSTR"):
                event_group_id = self.asgs_db_inst.insert_event_group(state_id, instance_id, msg_obj)
            else:
                # don't need a new event group
                self.logger.debug("Reusing event_group_id: %s", event_group_id)

                # update event group with this latest state
                # added 3/6/19 - will set status to EXIT if this is a FEND or REND event_type
                # will hardcode this state id for now, until I get my messaging refactor delivered
                if event_name in ['FEND', 'REND']:
                    state_id = 9
                    self.logger.debug("Got FEND event type: setting state_id to %s", str(state_id))

                    self.asgs_db_inst.update_event_group(state_id, event_group_id, msg_obj)

            # now insert message into the event table
            self.asgs_db_inst.insert_event(site_id[0], event_group_id, event_type_id, msg_obj)
        else:
            self.logger.error("FAILURE - Cannot retrieve advisory number, site, event type or state type ids.")

    def asgs_run_props_callback(self, channel, method, properties, body):
        """
        The callback function for the run properties queue

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """

        # init the return message
        ret_msg = None

        self.logger.info("Received ASGS run props msg. Body is %s bytes.", len(body))
        context = "Run properties message queue callback function"

        # load the message
        try:
            # load the json
            msg_obj = json.loads(body)

            # get the site id from the name in the message
            site_id = self.asgs_constants_inst.get_lu_id_from_msg(msg_obj, "physical_location", "site")

            if site_id is None or site_id[0] < 0:
                err = f'ERROR Unknown physical location {msg_obj.get("physical_location", "")}, Ignoring message'

                self.logger.error(err)

                # send a message to slack
                self.general_utils.send_slack_msg(err, 'slack_issues_channel')
            else:
                self.logger.debug("site_id: %s", str(site_id))

                # filter out handing - accept runs for all locations, except UCF and George Mason runs for now
                site_ids = self.get_site_ids()

                # init the instance id
                instance_id: int = 0

                # check the site id
                if site_id[0] in site_ids:
                    # get the instance id
                    instance_id = self.asgs_db_inst.get_existing_instance_id(site_id[0], msg_obj)

                    self.logger.info("instance_id: %s", str(instance_id))

                    # we must have an existing instance id
                    if instance_id > 0:
                        # get the configuration params
                        param_list = msg_obj.get("param_list")

                        if param_list is not None:
                            # insert the records
                            ret_msg = self.asgs_db_inst.insert_config_items(instance_id, param_list)

                            if ret_msg is not None:
                                err = f'ERROR - DB insert for message failed: {ret_msg}. Ignoring message.'
                                self.logger.error(err)

                                # send a message to slack
                                self.general_utils.send_slack_msg(err, 'slack_issues_channel')

                        else:
                            err = "ERROR - Invalid message - 'param_list' key is missing from the message. Ignoring message."
                            self.logger.error(err)

                            # send a message to slack
                            self.general_utils.send_slack_msg(err, 'slack_issues_channel')
                    else:
                        self.logger.error("FAILURE - Cannot find instance. Ignoring message.")

                        # send a message to slack
                        self.general_utils.send_slack_msg(context, 'slack_issues_channel', f'Instance provided in message: '
                                                                                           f'{msg_obj.get("instance_name", "N/A")} '
                                                                                           f'does not exist. Ignoring message.')
                else:
                    self.logger.error('FAILURE - Site %s not supported. Ignoring message.', {site_id[1]})

        except Exception:
            self.logger.exception("ERROR loading the config message.")

            # send a message to slack
            self.general_utils.send_slack_msg(context, 'slack_issues_channel', "ERROR loading the config message.")

    def ecflow_run_props_callback(self, channel, method, properties, body):
        """
        The callback function for the ecflow run properties queue

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """

        self.logger.info("Received ECFlow run props msg. Body is %s bytes.", len(body))

    def start_consuming(self, callback):
        """
        Creates and starts consuming queue messages

        :param callback:
        :return:
        """
        try:
            # create a new queue message handler
            channel: pika.adapters.blocking_connection.BlockingChannel = self.create_msg_listener(self.queue_name)

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

    def create_msg_listener(self, queue_name: str) -> pika.adapters.blocking_connection.BlockingChannel:
        """
        Creates a new queue message listener

        :param queue_name:
        :return:
        """
        # init the return
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
            channel.queue_declare(queue=queue_name)

            self.logger.info('%s queue channel configured on %s:5672.', queue_name, os.environ.get("RABBITMQ_HOST"))
        except Exception:
            self.logger.exception("Error: Exception on the creation of channel to %s.", queue_name)

        # return the queue channel
        return channel

    def get_site_ids(self) -> list:
        """
        gets the list of site ids for the ASGS run properties message handler

        :return:
        """
        # get all the ids
        renci = self.asgs_constants_inst.get_lu_id('RENCI', 'site')
        tacc = self.asgs_constants_inst.get_lu_id('TACC', 'site')
        lsu = self.asgs_constants_inst.get_lu_id('LSU', 'site')
        penguin = self.asgs_constants_inst.get_lu_id('Penguin', 'site')
        loni = self.asgs_constants_inst.get_lu_id('LONI', 'site')
        seahorse = self.asgs_constants_inst.get_lu_id('Seahorse', 'site')
        qb2 = self.asgs_constants_inst.get_lu_id('QB2', 'site')
        cct = self.asgs_constants_inst.get_lu_id('CCT', 'site')
        psc = self.asgs_constants_inst.get_lu_id('PSC', 'site')

        # return the list of ids
        return [cct, loni, lsu, penguin, psc, qb2, renci, seahorse, tacc]

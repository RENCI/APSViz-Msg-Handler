# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    callback methods to handle posts to the RabbitMQ

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import json

from src.common.logger import LoggingUtil
from src.common.pg_impl import PGImplementation
from src.common.general_utils import GeneralUtils
from src.common.queue_utils import QueueUtils


class QueueCallbacks:
    """
    callback methods to handle posts to the RabbitMQ
    """

    def __init__(self, _queue_name, _logger=None):
        """
        init the queue message handler object for ASGS messages

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
            self.logger = LoggingUtil.init_logging("APSVIZ.Msg-Handler.QueueCallbacks", level=log_level, line_format='medium', log_file_path=log_path)

        self.logger.info("Initializing QueueCallback for queue %s", _queue_name)

        # specify the DB to get a connection to
        # note the extra comma makes this single item a singleton tuple
        self.db_names: tuple = ('asgs',)

        # define and init the object that will handle ASGS DB operations
        self.db_info: PGImplementation = PGImplementation(self.db_names, _logger=self.logger)

        # define and init the object used to handle ASGS constant conversions
        self.queue_utils = QueueUtils(_queue_name=_queue_name, _logger=self.logger)

        # create the general queue utilities class
        self.general_utils = GeneralUtils(_logger=self.logger)

        self.logger.info("QueueCallback initialization for queue %s complete.", _queue_name)

    def asgs_status_msg_callback(self, channel, method, properties, body) -> bool:
        """
        main worker that operates on the incoming ASGS status message queue

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger.debug("Received ASGS status msg. Body is %s bytes, channel: %s, method: %s, properties: %s.", len(body), channel, method,
                          properties)

        self.logger.debug('Received ASGS status msg %s', body)

        # init the return
        ret_val = True

        context = 'asgs_status_msg_callback()'

        # load the message
        try:
            # load the message
            msg_obj: json = json.loads(body)

            # get the site id from the name in the message
            site_id = self.db_info.get_lu_id_from_msg(msg_obj, "physical_location", "site", context=context)

            # get the 3vent type if from the event name in the message
            event_type_id, event_name = self.db_info.get_lu_id_from_msg(msg_obj, "event_type", "event_type", context=context)

            # get the event type if from the event name in the message
            state_id, state_name = self.db_info.get_lu_id_from_msg(msg_obj, "state", "state_type", context=context)

            # get the event advisory data
            advisory_id = msg_obj.get("advisory_number", "N/A") if (msg_obj.get("advisory_number", "N/A") != "") else "N/A"

            # did we get everything needed
            if site_id[0] >= 0 and event_type_id >= 0 and state_id >= 0 and advisory_id != 'N/A':
                # check to see if there are any instances for this site_id yet
                # this might happen if we start up this process in the middle of a model run
                instance_id = self.db_info.get_existing_instance_id(site_id[0], msg_obj)

                # if this is a STRT event, create a new instance
                if instance_id < 0 or (event_name == "STRT" and state_name == "RUNN"):
                    self.logger.debug("create_new_inst is True - creating new inst. context: %s", context)

                    # insert the record
                    instance_id = self.db_info.insert_instance(state_id, site_id[0], msg_obj, context)

                else:  # just update instance
                    self.logger.debug("create_new_inst is False - updating instance id. context: %s", context)

                    # update the instance
                    self.db_info.update_instance(state_id, site_id[0], instance_id, msg_obj)

                # if we don't have an instance id at this point we cant continue
                if instance_id < 0:
                    err_msg = f"{context}: Error - Cannot obtain a valid instance ID."

                    self.logger.error(err_msg)

                    # send a message to slack
                    self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                    # set the return to indicate failure
                    ret_val = False
                else:
                    # check to see if there are any event groups for this site_id and inst yet
                    # this might happen if we start up this process in the middle of a model run
                    event_group_id = self.db_info.get_existing_event_group_id(instance_id, advisory_id, context)

                    # if this is the start of a group of Events, create a new event_group record
                    # qualifying group initiation: event type = RSTR
                    # STRT & HIND do not belong to any event group??
                    # For now, it is required that every event belong to an event group, so I will add those as well.
                    # create a new event group if none exist for this site & instance yet or if starting a new cycle

                    # +++++++++++++++++++++++++ Figure out how to stop creating a second event group
                    #   after creating first one, when very first RSTR comes for this instance+++++++++++++++++++

                    if event_group_id < 0 or (event_name == "RSTR"):
                        event_group_id = self.db_info.insert_event_group(state_id, instance_id, msg_obj, context)
                    else:
                        # don't need a new event group
                        self.logger.debug("Reusing event_group_id: %s, context: %s", event_group_id, context)

                        # update event group with this latest state
                        # added 3/6/19 - will set status to EXIT if this is a FEND or REND event_type
                        # will hardcode this state id for now, until I get my messaging refactor delivered
                        if event_name in ['FEND', 'REND']:
                            state_id = 9
                            self.logger.debug("Got FEND event type: setting state_id to %s, context: %s", str(state_id), context)

                            self.db_info.update_event_group(state_id, event_group_id, msg_obj)

                    # now insert message into the event table
                    self.db_info.insert_event(site_id[0], event_group_id, event_type_id, msg_obj, context)

                    # relay the msg if enabled
                    ret_val = self.queue_utils.relay_msg(body)

                    # alert on failure
                    if not ret_val:
                        # create an error message
                        err_msg = f"{context}: Error - Failure to relay message for instance id: {instance_id}."

                        self.logger.error(err_msg)

                        # send a message to slack
                        self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')
            else:
                err_msg = f"{context}: Error - Cannot retrieve advisory number, site, event type or state type ids."

                self.logger.error(err_msg)

                # send a message to slack
                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                # set the return to indicate failure
                ret_val = False

        except Exception:
            err_msg = f"{context}: Error loading the ASGS status message"

            self.logger.exception(err_msg)

            # send a message to slack
            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

            # set the return to indicate failure
            ret_val = False

        # return the success flag
        return ret_val

    def asgs_run_props_callback(self, channel, method, properties, body) -> bool:
        """
        The callback function for the ASGS run properties message queue

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger.debug("Received ASGS run props msg. Body is %s bytes, channel: %s, method: %s, properties: %s", len(body), channel, method,
                          properties)

        self.logger.debug('Received ASGS run props msg: %s', body)

        # init the success flag
        ret_val = True

        context = "asgs_run_props_callback()"

        # load the message
        try:
            # load the json
            msg_obj: json = json.loads(body)

            # get the site id from the name in the message
            site_id = self.db_info.get_lu_id_from_msg(msg_obj, "physical_location", "site", context=context)

            # insure we have a legit location
            if site_id is None or site_id[0] < 0:
                err_msg: str = f'{context}: ERROR Unknown physical location {msg_obj.get("physical_location", "")}, Ignoring message.'

                self.logger.error(err_msg)

                # send a message to slack
                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                # set the return to indicate failure
                ret_val = False
            else:
                self.logger.debug("site_id: %s, context: %s", str(site_id), context)

                # filter out handing - accept runs for all locations, except UCF and George Mason runs for now
                site_ids: list = self.db_info.get_site_ids()

                # init the instance id
                instance_id: int = 0

                # check the site id
                if site_id[0] in site_ids:
                    # get the instance id
                    instance_id = self.db_info.get_existing_instance_id(site_id[0], msg_obj)

                    self.logger.debug("instance_id: %s, context: %s", str(instance_id), context)

                    # we must have an existing instance id
                    if instance_id > 0:
                        # add in the high level params
                        param_list: dict = {'physical_location': msg_obj['physical_location'], 'uid': msg_obj['uid'],
                                            'instance_name': msg_obj['instance_name'], 'workflow_type': 'ASGS', 'supervisor_job_status': 'new',
                                            'product_code': 'asgs', 'insertion_date': self.queue_utils.get_formatted_date()}

                        # convert the asgs object into a single dict (like ecflow and hecras)
                        param_list.update({x[0]: x[1] for x in msg_obj['param_list']})

                        # was there a param list found
                        if param_list is not None:
                            # insert the records
                            err_msg: str = self.db_info.insert_config_items(instance_id, param_list)

                            # was there an error
                            if err_msg is not None:
                                err_msg: str = f'{context}: Error - DB insert for run properties message failed: {err_msg}, ignoring message.'
                                self.logger.error(err_msg)

                                # send a message to slack
                                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                                # set the return to indicate failure
                                ret_val = False
                            else:
                                # relay the msg if enabled
                                ret_val = self.queue_utils.relay_msg(body)

                                # alert on failure
                                if not ret_val:
                                    # create an error message
                                    err_msg = f"{context}: Error - Failure to relay message for instance id: {instance_id}."

                                    self.logger.error(err_msg)

                                    # send a message to slack
                                    self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')
                        else:
                            err_msg: str = f"{context}: Error invalid message - 'param_list' key is missing from the run properties message. " \
                                           "Ignoring message."
                            self.logger.error(err_msg)

                            # send a message to slack
                            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                            # set the return to indicate failure
                            ret_val = False
                    else:
                        err_msg: str = f"{context}: Error invalid instance ID. Ignoring message for ASGS {msg_obj.get('physical_location', 'N/A')}."
                        self.logger.error(err_msg)

                        # send a message to slack
                        self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                        # set the return to indicate failure
                        ret_val = False
                else:
                    err_msg: str = f"{context}: Error - Site {site_id} not supported. Ignoring message."
                    self.logger.error(err_msg)

                    # send a message to slack
                    self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                    # set the return to indicate failure
                    ret_val = False
        except Exception:
            err_msg: str = f"{context}: Error loading the run properties message."
            self.logger.exception(err_msg)

            # send a message to slack
            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

            # set the return to indicate failure
            ret_val = False

        # return any error messages
        return ret_val

    def ecflow_run_time_status_callback(self, channel, method, properties, body) -> bool:
        """
        The callback function for the ecflow run time status message queue.

        Note: this is a nearly an exact clone of the asgs version above (asgs_status_msg_callback)
        in case there are any modifications that are needed to comply with the ecflow msg.

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger.debug("Received ECFlow_rt status msg. Body is %s bytes, channel: %s, method: %s, properties: %s", len(body), channel, method,
                          properties)

        self.logger.debug('Received ECFlow_rt msg %s', body)

        # init the return
        ret_val = True

        context = 'ecflow_run_time_status_callback()'

        # load the message
        try:
            # load the message
            msg_obj = json.loads(body)

            # get the site id from the name in the message
            site_id = self.db_info.get_lu_id_from_msg(msg_obj, "physical_location", "site", context=context)

            # get the 3vent type if from the event name in the message
            event_type_id, event_name = self.db_info.get_lu_id_from_msg(msg_obj, "event_type", "event_type", context=context)

            # get the event type if from the event name in the message
            state_id, state_name = self.db_info.get_lu_id_from_msg(msg_obj, "state", "state_type", context=context)

            # get the event advisory data
            advisory_id = msg_obj.get("advisory_number", "N/A") if (msg_obj.get("advisory_number", "N/A") != "") else "N/A"

            # did we get everything needed
            if site_id[0] >= 0 and event_type_id >= 0 and state_id >= 0 and advisory_id != 'N/A':
                # check to see if there are any instances for this site_id yet
                # this might happen if we start up this process in the middle of a model run
                instance_id = self.db_info.get_existing_instance_id(site_id[0], msg_obj)

                # if this is a STRT event, create a new instance
                if instance_id < 0 or (event_name == "STRT" and state_name == "RUNN"):
                    self.logger.debug("create_new_inst is True - creating new instance id, context: %s", context)

                    # insert the record
                    instance_id = self.db_info.insert_instance(state_id, site_id[0], msg_obj, context)

                else:  # just update instance
                    self.logger.debug("create_new_inst is False - updating instance id, context: %s", context)

                    # update the instance
                    self.db_info.update_instance(state_id, site_id[0], instance_id, msg_obj)

                # if we don't have an instance id at this point we cant continue
                if instance_id < 0:
                    err_msg = f"{context}: Error - Cannot obtain a valid instance ID."

                    self.logger.error(err_msg)

                    # send a message to slack
                    self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                    # set the return to indicate failure
                    ret_val = False
                else:
                    # check to see if there are any event groups for this site_id and inst yet
                    # this might happen if we start up this process in the middle of a model run
                    event_group_id = self.db_info.get_existing_event_group_id(instance_id, advisory_id, context)

                    # if this is the start of a group of Events, create a new event_group record
                    # qualifying group initiation: event type = RSTR
                    # STRT & HIND do not belong to any event group??
                    # For now, it is required that every event belong to an event group, so I will add those as well.
                    # create a new event group if none exist for this site & instance yet or if starting a new cycle

                    # +++++++++++++++++++++++++ Figure out how to stop creating a second event group
                    #   after creating first one, when very first RSTR comes for this instance+++++++++++++++++++

                    if event_group_id < 0 or (event_name == "RSTR"):
                        event_group_id = self.db_info.insert_event_group(state_id, instance_id, msg_obj, context)
                    else:
                        # don't need a new event group
                        self.logger.debug("Reusing event_group_id: %s, context: %s", event_group_id, context)

                        # update event group with this latest state
                        # added 3/6/19 - will set status to EXIT if this is a FEND or REND event_type
                        # will hardcode this state id for now, until I get my messaging refactor delivered
                        if event_name in ['FEND', 'REND']:
                            state_id = 9
                            self.logger.debug("Got FEND event type: setting state_id to %s, context: %s", str(state_id), context)

                            self.db_info.update_event_group(state_id, event_group_id, msg_obj)

                    # now insert message into the event table
                    self.db_info.insert_event(site_id[0], event_group_id, event_type_id, msg_obj, context)

                    # relay the msg if enabled
                    ret_val = self.queue_utils.relay_msg(body)

                    # alert on failure
                    if not ret_val:
                        # create an error message
                        err_msg = f"{context}: Error - Failure to relay message for instance id: {instance_id}."

                        self.logger.error(err_msg)

                        # send a message to slack
                        self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')
            else:
                err_msg = f"{context}: Error - Cannot retrieve advisory number, site, event type or state type ids."

                self.logger.error(err_msg)

                # send a message to slack
                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                # set the return to indicate failure
                ret_val = False
        except Exception:
            err_msg = f"{context}: Error loading the ECFLOW status message."

            self.logger.exception(err_msg)

            # send a message to slack
            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

            # set the return to indicate failure
            ret_val = False

        # return the success flag
        return ret_val

    def ecflow_run_props_callback(self, channel, method, properties, body) -> bool:
        """
        The callback function for the ecflow run properties message queue

        Note - the supervisor is expecting the following mappings:
        'adcirc.gridname' = suite.adcirc.gridname
        'instancename' = suite.instance_name
        'supervisor_job_status' = 'new'
        'forcing.stormname' = forcing.stormname
        'workflow_type' = 'ecflow'
        '%downloadurl%' = output.downloadurl

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger.debug("Received ECFlow_rp run props msg. Body is %s bytes, channel: %s, method: %s, properties: %s", len(body), channel, method,
                          properties)

        self.logger.debug('Received ECFlow_rp msg: %s', body)

        # init the returned success flag
        ret_val: bool = True

        # set the slack/log message context
        context: str = "ecflow_run_props_callback()"

        # load the message
        try:
            # load the json
            msg_obj: json = json.loads(body)

            # transform the ecflow messages into the asgs equivalent
            msg_obj = self.queue_utils.extend_msg_to_asgs_legacy(msg_obj)

            # get the site id from the name in the message
            site_id = self.db_info.get_lu_id_from_msg(msg_obj, "physical_location", "site", context=context)

            # insure we have a legit location
            if site_id is None or site_id[0] < 0:
                # create the error message
                err_msg: str = f'{context}: ERROR Unknown physical location {msg_obj.get("physical_location", "")}, Ignoring message'

                # log the event
                self.logger.error(err_msg)

                # send a message to slack
                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                # set the failure flag
                ret_val = False
            else:
                self.logger.debug("site_id: %s, context: %s", str(site_id), context)

                # filter out handing - accept runs for all locations, except UCF and George Mason runs for now
                site_ids: list = self.db_info.get_site_ids()

                # init the instance id
                instance_id: int = 0

                # check the site id
                if site_id[0] in site_ids:
                    # get the instance id
                    instance_id = self.db_info.get_existing_instance_id(site_id[0], msg_obj)

                    self.logger.debug("instance_id: %s", str(instance_id))

                    # we must have an existing instance id
                    if instance_id > 0:
                        # add params for the workflow type, supervisor startup flag and the insertion timestamp of this queue
                        msg_obj.update(
                            {'workflow_type': 'ECFLOW', 'supervisor_job_status': 'new', 'insertion_date': self.queue_utils.get_formatted_date()})

                        # insert the records
                        err_msg: str = self.db_info.insert_config_items(instance_id, msg_obj)

                        if err_msg is not None:
                            err_msg: str = f'{context}: Error - DB insert for run properties message failed: {err_msg}, ignoring message.'
                            self.logger.error(err_msg)

                            # send a message to slack
                            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                            # set the failure flag
                            ret_val = False
                        else:
                            # relay the msg if enabled
                            ret_val = self.queue_utils.relay_msg(body)

                            # alert on failure
                            if not ret_val:
                                # create an error message
                                err_msg = f"{context}: Error - Failure to relay message for instance id: {instance_id}."

                                self.logger.error(err_msg)

                                # send a message to slack
                                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')
                    else:
                        err_msg: str = f"{context}: Error invalid instance ID. Ignoring message for ECFLOW {msg_obj.get('physical_location', 'N/A')}."
                        self.logger.error(err_msg)

                        # send a message to slack
                        self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                        # set the failure flag
                        ret_val = False
                else:
                    err_msg: str = f"{context}: Error - Site {site_id} not supported. Ignoring message."
                    self.logger.error(err_msg)

                    # send a message to slack
                    self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                    # set the failure flag
                    ret_val = False
        except Exception:
            err_msg: str = f"{context}: Error loading the run properties message."
            self.logger.exception(err_msg)

            # send a message to slack
            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

            # set the failure flag
            ret_val = False

        # return the success flag
        return ret_val

    def hecras_run_props_callback(self, channel, method, properties, body) -> bool:
        """
        The callback function for the hec/ras run properties message queue

        Note - the supervisor is expecting the following mappings:
        'adcirc.gridname' = suite.adcirc.gridname
        'instancename' = suite.instance_name
        'supervisor_job_status' = 'new'
        'forcing.stormname' = forcing.stormname
        'workflow_type' = 'hecras'
        '%downloadurl%' = output.downloadurl

        :param channel:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.logger.debug("Received HEC/RAS msg. Body is %s bytes, channel: %s, method: %s, properties: %s", len(body), channel, method, properties)

        self.logger.debug('Received HEC/RAS msg %s', body)

        # init the success flag
        ret_val: bool = True

        # set the slack/log message context
        context: str = "hecras_run_props_callback()"

        # load the message
        try:
            # load the json
            msg_obj: json = json.loads(body)

            # transform the ecflow messages into the asgs equivalent
            msg_obj = self.queue_utils.extend_msg_to_asgs_legacy(msg_obj)

            # get the site id from the name in the message
            site_id = self.db_info.get_lu_id_from_msg(msg_obj, "physical_location", "site", context=context)

            # insure we have a legit location
            if site_id is None or site_id[0] < 0:
                # create the error message
                err_msg: str = f'{context}: ERROR Unknown physical location {msg_obj.get("physical_location", "")}, Ignoring message'

                # log the event
                self.logger.error(err_msg)

                # send a message to slack
                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                # set the failure flag
                ret_val = False
            else:
                self.logger.debug("site_id: %s, context: %s", str(site_id), context)

                # filter out handing - accept runs for all locations, except UCF and George Mason runs for now
                site_ids: list = self.db_info.get_site_ids()

                # init the instance id
                instance_id: int = 0

                # check the site id
                if site_id[0] in site_ids:
                    # get the instance id
                    instance_id = self.db_info.get_existing_instance_id(site_id[0], msg_obj)

                    self.logger.debug("instance_id: %s, context: %s", str(instance_id), context)

                    # we must have an existing instance id
                    if instance_id > 0:
                        # add params for the workflow type, supervisor startup flag and the insertion timestamp of this queue message
                        msg_obj.update(
                            {'workflow_type': 'HECRAS', 'supervisor_job_status': 'new', 'insertion_date': self.queue_utils.get_formatted_date()})

                        # insert the records
                        err_msg: str = self.db_info.insert_config_items(instance_id, msg_obj, '_HECRAS')

                        # was there a problem
                        if err_msg is not None:
                            err_msg: str = f'{context}: Error - DB insert for run properties message failed: {err_msg}, ignoring message.'
                            self.logger.error(err_msg)

                            # send a message to slack
                            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                            # set the failure flag
                            ret_val = False
                        else:
                            # relay the msg if enabled
                            ret_val = self.queue_utils.relay_msg(body)

                            # alert on failure
                            if not ret_val:
                                # create an error message
                                err_msg = f"{context}: Error - Failure to relay message for instance id: {instance_id}."

                                self.logger.error(err_msg)

                                # send a message to slack
                                self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')
                    else:
                        err_msg: str = f"{context}: Error invalid instance ID. Ignoring message for HEC/RAS " \
                                       f"{msg_obj.get('physical_location', 'N/A')}."

                        self.logger.error(err_msg)

                        # send a message to slack
                        self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                        # set the failure flag
                        ret_val = False
                else:
                    err_msg: str = f"{context}: Error - Site {site_id} not supported. Ignoring message."
                    self.logger.error(err_msg)

                    # send a message to slack
                    self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

                    # set the failure flag
                    ret_val = False
        except Exception:
            err_msg: str = f"{context}: Error loading the run properties message."
            self.logger.exception(err_msg)

            # send a message to slack
            self.general_utils.send_slack_msg(err_msg, 'slack_issues_channel')

            # set the failure flag
            ret_val = False

        # return any error messages
        return ret_val

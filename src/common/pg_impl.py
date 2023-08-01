# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class for database functionalities

    Author: Phil Owen, RENCI.org
"""
import datetime

from src.common.pg_utils_multi import PGUtilsMultiConnect
from src.common.logger import LoggingUtil
from src.common.queue_utils import QueueUtils


class PGImplementation(PGUtilsMultiConnect):
    """
        Class that contains DB calls for the message handler.

        Note this class inherits from the PGUtilsMultiConnect class
        which has all the connection and cursor handling.
    """

    def __init__(self, db_names: tuple, _logger=None, _auto_commit=True):
        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("APSViz.Msg-Handler.PGImplementation", level=log_level, line_format='medium',
                                                   log_file_path=log_path)

        # create the general queue utilities class
        self.queue_utils = QueueUtils(_queue_name='', _logger=self.logger)

        # init the base class
        PGUtilsMultiConnect.__init__(self, 'APSViz.Settings', db_names, _logger=self.logger, _auto_commit=_auto_commit)

        # load the ASGS constants into memory
        self.asgs_constants = self.build_asgs_constants()

    def __del__(self):
        """
        Calls super base class to clean up DB connections and cursors.

        :return:
        """
        # clean up connections and cursors
        PGUtilsMultiConnect.__del__(self)

    def build_asgs_constants(self) -> dict:
        # create a list of target lu tables
        lu_tables = ['ASGS_Mon_site_lu', 'ASGS_Mon_event_type_lu', 'ASGS_Mon_state_type_lu', 'ASGS_Mon_instance_state_type_lu']

        # init the lu_data storage
        lu_data: dict = {}

        # make the call to get the data
        for lu_item in lu_tables:
            lu_data.update({lu_item.removeprefix('ASGS_Mon_').removesuffix('_lu'): self.get_lu_items(lu_item)})

        # add in the pct_complete items
        lu_data.update(
            {'pct_complete': {'0': 0, '1': 5, '2': 20, '3': 40, '4': 60, '5': 90, '6': 100, '7': 0, '8': 0, '9': 0, '10': 40, '11': 90, '12': 20}})

        # return the data
        return lu_data

    def get_lu_items(self, lu_name: str):
        """
        gets the lookup items for the table name passed.

        :param lu_name:
        :return:
        """
        # init the return value
        ret_val = None

        # create the sql to call the stored function
        sql_stmt = f"SELECT * FROM public.get_lu_items(lu_name := '{lu_name}')"

        # get the data
        lu_data = self.exec_sql('asgs', sql_stmt)

        # check the return
        if lu_data != -1:
            # assign the json object to the return
            ret_val = lu_data

        # return to the caller
        return lu_data

    def get_lu_id_from_msg(self, msg_obj, param_name: str, lu_name: str, context: str = 'unknown'):
        """
        gets the lookup entry for the param/type passed.

        :param msg_obj:
        :param param_name:
        :param lu_name:
        :param context:
        :return:
        """
        # get the name
        ret_name = msg_obj.get(param_name, "")

        # get the ID
        ret_id = self.get_lu_id(ret_name, lu_name, context)

        # did we find something
        if ret_id >= 0:
            self.logger.debug("PASS - LU name: %s, Param name: %s, ID: %s, context: %s", lu_name, param_name, str(ret_id), context)
        else:
            self.logger.error("FAILURE - Invalid or no param name: %s not found in: %s, context: %s", param_name, lu_name, context)

        # return to the caller
        return ret_id, ret_name

    def get_lu_id(self, element_name, lu_name, context: str = 'unknown'):
        """
        gets the id from a lookup table

        :param element_name:
        :param lu_name:
        :param context:
        :return:
        """
        # get the ID
        ret_id = self.asgs_constants[lu_name].get(element_name, -1)

        # did we find something
        if ret_id >= 0:
            self.logger.debug("PASS - LU name: %s, element name: %s, ID: %s, context: %s", lu_name, element_name, str(ret_id), context)
        else:
            self.logger.error("FAILURE - Invalid or no element name: %s not found in: %s, context: %s", element_name, lu_name, context)

        # return to the caller
        return ret_id

    def get_site_ids(self, context='unknown') -> list:
        """
        gets the list of site ids for the ASGS run properties message handler

        :return:
        """
        # return the list of ids
        return [value for key, value in self.asgs_constants['site'].items()]

    def get_existing_event_group_id(self, instance_id, advisory_id, context: str = 'unknown'):
        """
        just a check to see if there are any event groups defined for this site yet

        :param instance_id:
        :param advisory_id:
        :param context:
        :return:
        """
        self.logger.debug("instance_id: %s, advisory_id %s, context: %s", instance_id, advisory_id, context)

        # see if there are any event groups yet that have this instance_id
        # this could be caused by a new install that does not have any data in the DB yet
        sql_stmt = f"SELECT id FROM \"ASGS_Mon_event_group\" WHERE instance_id={instance_id} AND advisory_id='{advisory_id}' ORDER BY id DESC"

        group = self.exec_sql('asgs', sql_stmt)

        if group > 0:
            existing_group_id = group
        else:
            existing_group_id = -1

        self.logger.debug("existing_group_id: %s, context: %s", existing_group_id, context)

        return existing_group_id

    def get_existing_instance_id(self, site_id, msg_obj):
        """
        just a check to see if there are any instances defined for this site yet

        :param site_id:
        :param msg_obj:
        :return:
        """

        self.logger.debug("site_id: %s", site_id)

        # get the instance name from the message
        instance_name = msg_obj.get("instance_name", "N/A") if (msg_obj.get("instance_name", "N/A") != "") else "N/A"

        # get the process id from the message
        process_id = int(msg_obj.get("uid", "0")) if (msg_obj.get("uid", "0") != "") else 0

        # see if there are any instances yet that have this site_id and instance_name
        # this could be caused by a new install that does not have any data in the DB yet
        sql_stmt = f"SELECT id FROM \"ASGS_Mon_instance\" WHERE site_id={site_id} AND process_id={process_id} AND instance_name='{instance_name}' " \
                   f"AND inst_state_type_id!=9"

        # +++++++++++++++FIX THIS++++++++++++++++++++Add query to get correct stat id for Defunct++++++++++++++++++++++++
        # +++++++++++++++FIX THIS++++++++++++++++++++Add day to query too? (to account for rollover of process ids)++++++++++++++++++++++++

        # get the instance id if it exists
        inst = self.exec_sql('asgs', sql_stmt)

        # any int > 0 is a valid instance id
        if inst > 0:
            # save the existing instance ID
            existing_instance_id = inst
        else:
            self.logger.warning('Warning - Could not find Instance ID. Site id: %s, Process id: %s, Instance name: %s', site_id, process_id,
                                instance_name)

            # set the error code
            existing_instance_id = -1

        self.logger.debug("existing_instance_id: %s", existing_instance_id)

        return existing_instance_id

    def get_instance_id(self, start_ts, site_id, process_id, instance_name):
        """
        gets the instance id for a process

        :param start_ts:
        :param site_id:
        :param process_id:
        :param instance_name:
        :return:
        """
        self.logger.debug("start_ts: %s, site_id: %s, process_id: %s, instance_name: %s", start_ts, site_id, process_id, instance_name)

        # build up the sql statement to
        sql_stmt = f"SELECT id FROM \"ASGS_Mon_instance\" WHERE CAST(start_ts as DATE)='{start_ts[:10]}' AND site_id={site_id} AND " \
                   f"process_id={process_id} AND instance_name='{instance_name}'"

        inst = self.exec_sql('asgs', sql_stmt)

        if inst is not None:
            _id = inst
        else:
            _id = -1

        self.logger.debug("returning id: %s", _id)

        return id

    def update_event_group(self, state_id, event_group_id, msg_obj):
        """
        update the event group

        :param state_id:
        :param event_group_id:
        :param msg_obj:
        :return:
        """
        # get the storm name
        storm_name = msg_obj.get("storm", "N/A") if (msg_obj.get("storm", "N/A") != "") else "N/A"

        # get the advisory id
        advisory_id = msg_obj.get("advisory_number", "N/A") if (msg_obj.get("advisory_number", "N/A") != "") else "N/A"

        # build up the sql statement to update the event group
        sql_stmt = f"UPDATE \"ASGS_Mon_event_group\" SET state_type_id ={state_id}, storm_name='{storm_name}', advisory_id='{advisory_id}' " \
                   f"WHERE id={event_group_id} RETURNING 1"

        self.exec_sql('asgs', sql_stmt)

    def update_instance(self, state_id, site_id, instance_id, msg_obj):
        """
        update instance with the latest state_type_id

        :param state_id:
        :param site_id:
        :param instance_id:
        :param msg_obj:
        :return:
        """
        # get a default time stamp, use it if necessary
        now = datetime.datetime.now()
        time_stamp = now.strftime("%Y-%m-%d %H:%M")

        end_ts = msg_obj.get("date-time", time_stamp) if (msg_obj.get("date-time", time_stamp) != "") else time_stamp

        # get the run params
        run_params = msg_obj.get("run_params", "N/A") if (msg_obj.get("run_params", "N/A") != "") else "N/A"

        # build up the sql statement to update the instance
        sql_stmt = f"UPDATE \"ASGS_Mon_instance\" SET inst_state_type_id = {state_id}, end_ts = '{end_ts}', run_params = '{run_params}' " \
                   f"WHERE site_id = {site_id} AND id={instance_id} RETURNING 1"

        self.exec_sql('asgs', sql_stmt)

    def save_raw_msg(self, msg):
        """
        saves the raw message

        :param msg:
        :return:
        """
        self.logger.debug("msg: %s", msg)

        # build up the sql statement to insert the json data
        sql_stmt = f"INSERT INTO \"ASGS_Mon_json\" (data) VALUES ('{msg}') RETURNING 1"

        self.exec_sql('asgs', sql_stmt)

    def insert_event(self, site_id, event_group_id, event_type_id, msg_obj, context: str = 'unknown'):
        """
        process the message data and insert an event

        :param site_id:
        :param event_group_id:
        :param event_type_id:
        :param msg_obj:
        :param context:
        :return:
        """
        # get a default time stamp, use it if necessary
        now = datetime.datetime.now()
        time_stamp = now.strftime("%Y-%m-%d %H:%M")
        event_ts = msg_obj.get("date-time", time_stamp) if (msg_obj.get("date-time", time_stamp) != "") else time_stamp

        # get the event advisory data
        advisory_id = msg_obj.get("advisory_number", "N/A") if (msg_obj.get("advisory_number", "N/A") != "") else "N/A"

        # get the process data
        process = msg_obj.get("process", "N/A") if (msg_obj.get("process", "N/A") != "") else "N/A"

        # get the percent complete from a LU lookup
        pct_complete = self.get_lu_id(str(event_type_id), "pct_complete", context)

        # get the sub percent complete from the message object
        sub_pct_complete = msg_obj.get("subpctcomplete", pct_complete)

        # if there was a message included parse and add it
        if msg_obj.get("message") is not None and len(msg_obj["message"]) > 0:
            # get rid of any special chars that might mess up postgres
            # backslashes, quote, and double quotes for now
            msg_line = msg_obj["message"].replace('\\', '').replace("'", '').replace('"', '')

            raw_data_col = ", raw_data"
            msg_line = f", '{msg_line}'"
        else:
            raw_data_col = ''
            msg_line = ''

        # create the fields
        sql_stmt = 'INSERT INTO "ASGS_Mon_event" (site_id, event_group_id, event_type_id, event_ts, advisory_id, pct_complete, sub_pct_complete, ' \
                   f"process{raw_data_col}) VALUES ({site_id}, {event_group_id}, {event_type_id}, '{event_ts}', '{advisory_id}', {pct_complete}, " \
                   f"{sub_pct_complete}, '{process}'{msg_line}) RETURNING 1"

        self.exec_sql('asgs', sql_stmt)

    def insert_event_group(self, state_id, instance_id, msg_obj, context: str = 'unknown'):
        """
        inserts an event group

        :param state_id:
        :param instance_id:
        :param msg_obj:
        :param context:
        :return:
        """
        # get a default time stamp, use it if necessary
        now = datetime.datetime.now()
        time_stamp = now.strftime("%Y-%m-%d %H:%M")
        event_group_ts = msg_obj.get("date-time", time_stamp) if (msg_obj.get("date-time", time_stamp) != "") else time_stamp

        # get the storm name
        storm_name = msg_obj.get("storm", "N/A") if (msg_obj.get("storm", "N/A") != "") else "N/A"

        # get the storm number
        storm_number = msg_obj.get("storm_number", "N/A") if (msg_obj.get("storm_number", "N/A") != "") else "N/A"

        # get the event advisory data
        advisory_id = msg_obj.get("advisory_number", "N/A") if (msg_obj.get("advisory_number", "N/A") != "") else "N/A"

        # build up the sql statement to insert the event
        sql_stmt = 'INSERT INTO "ASGS_Mon_event_group" (state_type_id, instance_id, event_group_ts, storm_name, storm_number, advisory_id, ' \
                   f"final_product) VALUES ({state_id}, {instance_id}, '{event_group_ts}', '{storm_name}', '{storm_number}', '{advisory_id}'" \
                   f", 'product') RETURNING id"

        # get the new event group id
        group = self.exec_sql('asgs', sql_stmt)

        self.logger.debug("group: %s, context: %s", group, context)

        # return the new event group id
        return group

    def insert_instance(self, state_id, site_id, msg_obj, context: str = 'unknown'):
        """
        inserts an instance

        id | process_id | start_ts | end_ts | run_params | inst_state_type_id | site_id  | instance_name

        :param state_id:
        :param site_id:
        :param msg_obj:
        :param context:
        :return:
        """
        # get a default time stamp, use it if necessary
        now = datetime.datetime.now()
        time_stamp = now.strftime("%Y-%m-%d %H:%M")
        start_ts = end_ts = msg_obj.get("date-time", time_stamp) if (msg_obj.get("date-time", time_stamp) != "") else time_stamp

        # get the run params
        run_params = msg_obj.get("run_params", "N/A") if (msg_obj.get("run_params", "N/A") != "") else "N/A"

        # get the instance name
        instance_name = msg_obj.get("instance_name", "N/A") if (msg_obj.get("instance_name", "N/A") != "") else "N/A"

        # get the process id
        process_id = int(msg_obj.get("uid", "0")) if (msg_obj.get("uid", "0") != "") else 0

        # check to make sure this instance doesn't already exist before adding a new one
        # instance_id = get_instance_id(start_ts, site_id, process_id, instance_name)
        # if (instance_id < 0):

        # build up the sql statement to insert the run instance
        sql_stmt = f"INSERT INTO \"ASGS_Mon_instance\" (site_id, process_id, start_ts, end_ts, run_params, instance_name, inst_state_type_id) " \
                   f"VALUES ({site_id}, {process_id}, '{start_ts}', '{end_ts}', '{run_params}', '{instance_name}', {state_id}) RETURNING id"

        # insert the record and the new instance id
        instance_id = self.exec_sql('asgs', sql_stmt)

        self.logger.debug("instance_id: %s, context: %s", instance_id, context)

        return instance_id

    def insert_config_items(self, instance_id: int, params: dict):
        """
        Inserts the configuration parameters into the database

        :param instance_id:
        :param params:
        :return:
        """

        # init the return value
        ret_msg = None

        # apply data formatting to the run props
        params = self.queue_utils.transform_msg_params(params)

        self.logger.debug("param_list: %s", params)

        # get advisory and enstorm values from param_list to create UID
        try:
            # get the advisory param
            advisory = params['advisory']

            # get the enstorm value
            enstorm = params['enstorm']

            # confirm we have the params
            if not advisory or not enstorm:
                ret_msg = "Error: 'advisory' and/or 'enstorm' parameters not found."
                self.logger.error(ret_msg)
            else:
                # build up the unique ID
                uid = str(advisory) + "-" + str(enstorm)

                self.logger.debug("uid: %s", uid)

                # build up the sql to remove old entries
                sql_stmt = f"DELETE FROM public.\"ASGS_Mon_config_item\" WHERE instance_id = {instance_id} AND uid = '{uid}' RETURNING 1"

                self.logger.debug("sql_stmt: %s", sql_stmt)

                # remove all duplicate records that may already exist
                self.exec_sql('asgs', sql_stmt)

                # get the list of values
                values_list = [f"({instance_id}, '{uid}', '{k}', '{v}')" for (k, v) in params.items()]

                # create a massive insert statement
                sql_stmt = f"INSERT INTO public.\"ASGS_Mon_config_item\" (instance_id, uid, key, value) VALUES {','.join(values_list)}"

                # insure this call returns a value
                sql_stmt += " RETURNING 1"

                self.logger.debug("sql_stmt: %s", sql_stmt)

                # execute the sql
                self.exec_sql('asgs', sql_stmt)

        except Exception:
            ret_msg = "Exception inserting config items"
            self.logger.exception(ret_msg)
            return ret_msg

        # return to the caller
        return ret_msg

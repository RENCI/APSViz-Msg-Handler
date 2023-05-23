# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class of helper methods to handle extracting elements from msg objects

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""

from src.common.logger import LoggingUtil


class AsgsConstants:
    """
    helper methods to handle lookup elements from msg objects

    """

    # define the LU constant lookups
    pct_complete_lu = {'0': 0, '1': 5, '2': 20, '3': 40, '4': 60, '5': 90, '6': 100, '7': 0, '8': 0, '9': 0, '10': 40, '11': 90, '12': 20}
    site_lu = {'RENCI': 0, 'TACC': 1, 'LSU': 2, 'UCF': 3, 'George Mason': 4, 'Penguin': 5, 'LONI': 6, 'Seahorse': 7, 'QB2': 8, 'CCT': 9, 'PSC': 10,
               'UGA': 10}
    event_type_lu = {'RSTR': 0, 'PRE1': 1, 'NOWC': 2, 'PRE2': 3, 'FORE': 4, 'POST': 5, 'REND': 6, 'STRT': 7, 'HIND': 8, 'EXIT': 9, 'FSTR': 10,
                     'FEND': 11, 'PNOW': 12}
    state_type_lu = {'INIT': 0, 'RUNN': 1, 'PEND': 2, 'FAIL': 3, 'WARN': 4, 'IDLE': 5, 'CMPL': 6, 'NONE': 7, 'WAIT': 8, 'EXIT': 9, 'STALLED': 10}
    instance_state_type_lu = {'INIT': 0, 'RUNN': 1, 'PEND': 2, 'FAIL': 3, 'WARN': 4, 'IDLE': 5, 'CMPL': 6, 'NONE': 7, 'WAIT': 8, 'EXIT': 9,
                              'STALLED': 10}

    # define string lookup of LU name array
    lus: dict = {"pct_complete": pct_complete_lu, "site": site_lu, "event_type": event_type_lu, "state_type": state_type_lu,
                 "instance_state_type": instance_state_type_lu}

    def __init__(self, _logger=None):
        """
        Init the class

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
            self.logger = LoggingUtil.init_logging("APSVIZ.Msg-Handler.ASGSConstants", level=log_level, line_format='medium', log_file_path=log_path)

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
        ret_id = self.lus[lu_name].get(element_name, -1)

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
        # get all the ids
        renci = self.get_lu_id('RENCI', 'site', context)
        tacc = self.get_lu_id('TACC', 'site', context)
        lsu = self.get_lu_id('LSU', 'site', context)
        penguin = self.get_lu_id('Penguin', 'site', context)
        loni = self.get_lu_id('LONI', 'site', context)
        seahorse = self.get_lu_id('Seahorse', 'site', context)
        qb2 = self.get_lu_id('QB2', 'site', context)
        cct = self.get_lu_id('CCT', 'site', context)
        psc = self.get_lu_id('PSC', 'site', context)
        uga = self.get_lu_id('UGA', 'site', context)

        # return the list of ids
        return [cct, loni, lsu, penguin, psc, qb2, renci, seahorse, tacc, uga]

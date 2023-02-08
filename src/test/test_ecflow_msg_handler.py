# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Test ecflow message handler - Tests the handling of an ecflow message

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import os
import json

from src.common.queue_utils import QueueUtils
from src.common.asgs_db import AsgsDb
from src.common.asgs_constants import AsgsConstants


def test_insert_ecflow_config_items():
    """
    Tests the parsing of ecflow data

    :return:
    """
    # define and init the object used to handle ASGS constant conversions
    asgs_constants = AsgsConstants(_logger=None)

    # define and init the object that will handle ASGS DB operations
    asgs_db = AsgsDb(asgs_constants, _logger=None)

    # instantiate the utility class
    queue_utils = QueueUtils(_queue_name='', _logger=None)

    # load the json
    with open(os.path.join(os.path.dirname(__file__), 'test_ecflow_run_props.json'), encoding='UTF-8') as test_fh:
        run_props = json.loads(test_fh.read())

    # transform the ecflow params into addition al asgs params
    ret_val = queue_utils.transform_ecflow_to_asgs(run_props)

    # add in the expected transformations
    run_props.update({'physical_location': 'ht-ncfs.renci.org', 'monitoring.rmqmessaging.locationname': 'ht-ncfs.renci.org', 'instance_name': 'ec95d',
                      'instancename': 'ec95d', 'uid': '90161888', 'ADCIRCgrid': 'ec95d', 'adcirc.gridname': 'ec95d', 'currentdate': '230206',
                      'currentcycle': '12', 'advisory': 'NA', 'asgs.enstorm': 'gfsforecast', 'enstorm': 'gfsforecast', 'stormname': 'none',
                      'forcing.tropicalcyclone.stormname': 'none', 'config.coupling.waves': '0',
                      'downloadurl': 'https://apsviz-thredds-dev.apps.renci.org/thredds/fileServer/2023/gfs/2023020612/ec95d/ht-ncfs.renci.org/ec95d'
                                     '/gfsforecast', 'forcing.tropicalcyclone.vortexmodel': 'NA'})

    # check the result
    assert ret_val == run_props

    # get the state type id. lets just set this to running for this test run
    state_id = asgs_constants.get_lu_id('RUNN', "state_type")

    # get the site id
    site_id = asgs_constants.get_lu_id(run_props.get('suite.physical_location'), 'site')

    # insert an instance record into the DB to get things primed
    instance_id = asgs_db.insert_instance(state_id, site_id, run_props)

    # insert the run params into the DB
    ret_val = asgs_db.insert_ecflow_config_items(instance_id, run_props, 'debug')

    # test the result, empty str == success
    assert ret_val is None

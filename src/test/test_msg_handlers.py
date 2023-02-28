# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
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
from src.common.queue_callbacks import QueueCallbacks

# these are the currently expected ecflow params that were transformed into ASGS legacy params
ecflow_expected_transformed_params: dict = {'physical_location': 'RENCI', 'monitoring.rmqmessaging.locationname': 'RENCI',
                                            'instance_name': 'ec95d', 'instancename': 'ec95d', 'uid': '90161888', 'ADCIRCgrid': 'ec95d',
                                            'adcirc.gridname': 'ec95d', 'currentdate': '230206', 'currentcycle': '12', 'advisory': 'NA',
                                            'asgs.enstorm': 'gfsforecast', 'enstorm': 'gfsforecast', 'stormname': 'none',
                                            'forcing.tropicalcyclone.stormname': 'none', 'config.coupling.waves': '0',
                                            'downloadurl': 'https://apsviz-thredds-dev.apps.renci.org/thredds/fileServer/2023/gfs/2023020612/' 
                                            'ec95d/ht-ncfs.renci.org/ec95d/gfsforecast', 'forcing.tropicalcyclone.vortexmodel': 'NA'}

# these are the currently expected hecras params that were transformed into ASGS legacy params
hecras_expected_transformed_params: dict = {'ADCIRCgrid': 'ec95d', 'adcirc.gridname': 'ec95d', 'advisory': 'NA', 'asgs.enstorm': 'gfsforecast',
                                            'config.coupling.waves': '0', 'currentcycle': '06', 'currentdate': '230206',
                                            'downloadurl': '//hecrastest/max_wse.tif', 'enstorm': 'gfsforecast', 'forcing.advisory': 'NA',
                                            'forcing.ensemblename': 'gfsforecast', 'forcing.metclass': 'synoptic', 'forcing.stormname': None,
                                            'forcing.tropicalcyclone.stormname': None, 'forcing.tropicalcyclone.vortexmodel': 'NA',
                                            'forcing.vortexmodel': 'NA', 'forcing.waves': '0', 'instance_name': 'ec95d', 'instancename': 'ec95d',
                                            'monitoring.rmqmessaging.locationname': 'RENCI', 'output.downloadurl': '//hecrastest/max_wse.tif',
                                            'physical_location': 'RENCI', 'stormname': None, 'suite.adcirc.gridname': 'ec95d',
                                            'suite.instance_name': 'ec95d', 'suite.physical_location': 'RENCI',
                                            'suite.project_code': 'hecras_test_renci', 'suite.uid': '94836645', 'time.currentcycle': '06',
                                            'time.currentdate': '230206', 'uid': '94836645'}


def test_insert_ecflow_config_items():
    """
    Tests the parsing of ecflow data and insertion into the DB

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
    ret_val = queue_utils.transform_msg_to_asgs_legacy(run_props)

    # add in the expected transformations
    run_props.update(ecflow_expected_transformed_params)

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


def test_ecflow_queue_callback():
    """
    Tests the handling of a ecflow run time msg

    :return:
    """
    # load the json
    with open(os.path.join(os.path.dirname(__file__), 'test_ecflow_run_time.json'), encoding='UTF-8') as test_fh:
        msg = test_fh.readline()

    # instantiate the utility class
    queue_callback = QueueCallbacks(_queue_name='', _logger=None)

    # call the msg handler callback
    success = queue_callback.ecflow_run_time_status_callback(None, None, None, msg)

    # check for pass/fail
    assert success is True


def test_insert_hecras_config_items():
    """
    Tests the parsing of hec/ras data and insertion into the DB

    :return:
    """
    # define and init the object used to handle ASGS constant conversions
    asgs_constants = AsgsConstants(_logger=None)

    # define and init the object that will handle ASGS DB operations
    asgs_db = AsgsDb(asgs_constants, _logger=None)

    # instantiate the utility class
    queue_utils = QueueUtils(_queue_name='', _logger=None)

    # load the json
    with open(os.path.join(os.path.dirname(__file__), 'test_hecras_run_props.json'), encoding='UTF-8') as test_fh:
        run_props = json.loads(test_fh.read())

    # transform the ecflow params into addition al asgs params
    ret_val = queue_utils.transform_msg_to_asgs_legacy(run_props)

    # add in the expected transformations
    run_props.update(hecras_expected_transformed_params)

    # check the result
    assert ret_val == run_props

    # get the state type id. lets just set this to running for this test run
    state_id = asgs_constants.get_lu_id('RUNN', "state_type")

    # get the site id
    site_id = asgs_constants.get_lu_id(run_props.get('suite.physical_location'), 'site')

    # insert an instance record into the DB to get things primed
    instance_id = asgs_db.insert_instance(state_id, site_id, run_props)

    # insert the run params into the DB
    ret_val = asgs_db.insert_hecras_config_items(instance_id, run_props, 'debug')

    # test the result, empty str == success
    assert ret_val is None


def test_hecras_queue_callback():
    """
    Tests the handling of a hec/ras run time msg

    :return:
    """
    # load the json
    with open(os.path.join(os.path.dirname(__file__), 'test_hecras_run_props.json'), encoding='UTF-8') as test_fh:
        msg = test_fh.readline()

    # instantiate the utility class
    queue_callback = QueueCallbacks(_queue_name='', _logger=None)

    # call the msg handler callback
    success = queue_callback.hecras_run_props_callback(None, None, None, msg)

    # check for pass/fail
    assert success is True

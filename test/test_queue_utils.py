# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Test Queue Utils - Test various queue utilities common to this project's components.

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import os
import json

from src.common.queue_utils import QueueUtils


def test_ecflow_transformer():
    """
    test the transformation of ecflow run params into asgs run params
    :return:
    """
    # load the test json
    with open(os.path.join(os.path.dirname(__file__), 'test_ecflow_run_props.json'), encoding='UTF-8') as test_fh:
        # load the json
        run_params = json.loads(test_fh.read())

        # instantiate the utility class
        queue_utils = QueueUtils(_queue_name='', _logger=None)

        # get the transformed list
        ret_val = queue_utils.transform_ecflow_to_asgs(run_params)

        assert (ret_val == {'suite.uid': '24306857', 'suite.instance_name': 'ncsc_coamps_09L', 'suite.physical_location': 'bridges2.psc.edu',
                            'suite.project': 'ncfs', 'suite.adcirc.grid': 'ncsc_sab_v1.23', 'time.currentdate': '20220928', 'time.currentcycle': '12',
                            'forcing.advisory': 'NA', 'forcing.ensemblename': 'coampsforecast', 'forcing.metclass': 'hybrid',
                            'forcing.stormname': '09L', 'forcing.stormnumber': '09', 'enstorm': 'coampsforecast',
                            'physical_location': 'bridges2.psc.edu', 'monitoring.rmqmessaging.locationname': 'bridges2.psc.edu',
                            'instance_name': 'ncsc_coamps_09L', 'instancename': 'ncsc_coamps_09L', 'uid': '24306857', 'ADCIRCgrid': 'ncsc_sab_v1.23',
                            'adcirc.gridname': 'ncsc_sab_v1.23', 'currentdate': '20220928', 'currentcycle': '12', 'advisory': 'NA',
                            'asgs.enstorm': 'coampsforecast', 'stormname': '09L', 'forcing.tropicalcyclone.stormname': '09L',
                            'downloadurl': 'thredds_data/2022/coamps_09L/2022092812/ncsc_sab_v1.23/bridges2.psc.edu/ncsc_coamps_09L',
                            'output.downloadurl': 'thredds_data/2022/coamps_09L/2022092812/ncsc_sab_v1.23/bridges2.psc.edu/ncsc_coamps_09L'})

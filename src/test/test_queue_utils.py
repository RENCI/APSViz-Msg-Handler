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
        run_props = json.loads(test_fh.read())

        # instantiate the utility class
        queue_utils = QueueUtils(_queue_name='', _logger=None)

        # get the transformed list
        ret_val = queue_utils.transform_ecflow_to_asgs(run_props)

        # add in the expected transformations
        run_props.update(
            {'physical_location': 'ht-ncfs.renci.org', 'monitoring.rmqmessaging.locationname': 'ht-ncfs.renci.org', 'instance_name': 'ec95d',
             'instancename': 'ec95d', 'uid': '90161888', 'ADCIRCgrid': 'ec95d', 'adcirc.gridname': 'ec95d', 'currentdate': '230206',
             'currentcycle': '12', 'advisory': 'NA', 'asgs.enstorm': 'gfsforecast', 'enstorm': 'gfsforecast', 'stormname': 'none',
             'forcing.tropicalcyclone.stormname': 'none', 'config.coupling.waves': '0',
             'downloadurl': 'https://apsviz-thredds-dev.apps.renci.org/thredds/fileServer/2023/gfs/2023020612/ec95d/ht-ncfs.renci.org/ec95d'
                            '/gfsforecast', 'forcing.tropicalcyclone.vortexmodel': 'NA'})

        # check the result
        assert ret_val == run_props

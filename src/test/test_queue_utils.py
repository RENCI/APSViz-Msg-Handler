# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
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

import test_msg_handlers as msg_tester
from src.common.queue_utils import QueueUtils

# create some test data references so the test can loop
test_data: list = [['test_ecflow_run_props.json', msg_tester.ecflow_expected_transformed_params],
                   ['test_hecras_run_props.json', msg_tester.hecras_expected_transformed_params]]


def test_asgs_legacy_transformer():
    """
    test the transformation of run params into asgs run params
    :return:
    """
    # for each set of test data
    for test_datum in test_data:
        # load the test json
        with open(os.path.join(os.path.dirname(__file__), test_datum[0]), encoding='UTF-8') as test_fh:
            # load the json
            run_props = json.loads(test_fh.read())

            # instantiate the utility class
            queue_utils = QueueUtils(_queue_name='', _logger=None)

            # get the transformed list
            ret_val = queue_utils.transform_msg_to_asgs_legacy(run_props)

            # add in the expected transformations. use the same expected params declared in the msg handler test
            run_props.update(test_datum[1])

            # check the result
            assert ret_val == run_props

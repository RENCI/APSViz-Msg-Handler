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

import test_ecflow_msg_handler as msg_tester
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

        # add in the expected transformations. use the same expected params declared in the msg handler test
        run_props.update(msg_tester.expected_transformed_params)

        # check the result
        assert ret_val == run_props

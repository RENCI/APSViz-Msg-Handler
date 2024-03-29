# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
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

from src.common.queue_utils import QueueUtils, ReformatType

# create some test data references so the test can loop
test_data: list = [['test_ecflow_run_props.json', msg_tester.ecflow_expected_transformed_params],
                   ['test_hecras_run_props.json', msg_tester.hecras_expected_transformed_params]]


def test_get_formatted_date():
    """
    tests the creation of a timestamp as a string

    :return:
    """
    # instantiate the utility class
    queue_utils = QueueUtils(_queue_name='')

    # get the transformed list
    ret_val: str = queue_utils.get_formatted_date()

    # check the result
    assert ret_val.endswith('+0000')


def test_is_enabled():
    """
    tests the logging of the relay enabled status

    :return:
    """

    # set the env param
    os.environ["RELAY_ENABLED"] = 'True'

    # instantiate the utility class. no queue name inhibits logging
    queue_utils = QueueUtils(_queue_name='')

    # get the relay status
    ret_val: bool = queue_utils.is_relay_enabled()

    # check the result. this is set on by default
    assert ret_val

    # unset the env param
    os.environ["RELAY_ENABLED"] = 'False'

    # instantiate the utility class
    queue_utils = QueueUtils(_queue_name='test')

    # get the relay status
    ret_val: bool = queue_utils.is_relay_enabled()

    # recheck the result
    assert not ret_val

    # recheck the result with a force
    # get the relay status
    ret_val: bool = queue_utils.is_relay_enabled(True)

    # check the result. this is set on by default
    assert ret_val


def test_legacy_extender():
    """
    test the transformation of run params into legacy params

    :return:
    """
    # for each set of test data
    for test_datum in test_data:
        # load the test json
        with open(test_datum[0], encoding='UTF-8') as test_fh:
            # load the json
            run_props = json.loads(test_fh.read())

            # instantiate the utility class
            queue_utils = QueueUtils(_queue_name='')

            # map the ECFLOW params to create legacy params
            ret_val = queue_utils.extend_msg_to_legacy_equivalent(run_props)

            # add in the expected transformations. use the same expected params declared in the msg handler test
            run_props.update(test_datum[1])

            # check the result
            assert ret_val == run_props


def test_transform_msg_params():
    """
    tests the reformation functionality

    :return:
    """
    # add in some test cases to transform
    test_params: dict = {'forcing.stormnumber': 'not a num', 'stormnumber': 'al03', 'storm': "1.5e10", 'stormname': 'LOWerCase',
                         'forcing.stormname': 'UPPERcase', 'forcing.tropicalcyclone.stormname': 'sentenceCASE', 'nokey': 'no_change',
                         'physical_location': 'no_change'}

    # create a dict of the expected transformations
    expected_params: dict = {'forcing.stormnumber': 'NaN', 'stormnumber': '03', 'storm': "15000000000.0", 'stormname': 'lowercase',
                             'forcing.stormname': 'UPPERCASE', 'forcing.tropicalcyclone.stormname': 'Sentencecase', 'nokey': 'no_change',
                             'physical_location': 'no_change'}

    # instantiate the utility class
    queue_utils = QueueUtils(_queue_name='')

    # make up some tests
    queue_utils.msg_transform_params = {'forcing.stormnumber': ReformatType.MAKE_INT, 'storm': ReformatType.FLOAT,
                                        'stormname': ReformatType.LOWERCASE, 'stormnumber': ReformatType.MAKE_INT,
                                        'forcing.stormname': ReformatType.UPPERCASE, 'forcing.tropicalcyclone.stormname': ReformatType.SENTENCE_CASE}

    # get the transformed list
    ret_val = queue_utils.transform_msg_params(test_params)

    # check the result
    assert ret_val == expected_params


def test_relay():
    """
    tests the message relay method

    there is a matrix of possible conditions here:
        level 1: A 'norelay' file can exist or not. when the file exists, it will stop all relaying.
        Level 2: RELAY host information (3 env params) must exist
        Level 3: RELAY_ENABLED environment param can be true/false or
                 force (true/false) can be passed to the relay call that can override RELAY_ENABLED


    :return:
    """
    queue_utils = QueueUtils(_queue_name='test')

    # test level 1, create the no-relay file in the root of the project
    with open(os.path.join('../../../', 'norelay'), 'x', encoding='UTF-8') as f_p:
        f_p.close()

    # send the msg to the queue specified
    ret_val: bool = queue_utils.relay_msg(b'{"test": "test"}')

    # check the result
    assert ret_val

    # remove the override relay file
    os.remove(os.path.join('../../../', 'norelay'))

    # test level 2, remove one of the relay host config items
    os.environ.pop("RELAY_RABBITMQ_HOST")

    # send the msg to the queue specified
    ret_val: bool = queue_utils.relay_msg(b'{"test": "test"}')

    # check the result
    assert ret_val

    # replace the relay host config items
    os.environ['RELAY_RABBITMQ_HOST'] = 'invalid_host_name'

    # test level 3, part 1. disable the relay, call the method with force off
    os.environ['RELAY_ENABLED'] = 'false'

    # send the msg to the queue specified
    ret_val: bool = queue_utils.relay_msg(b'{"test": "test"}')

    # check the result
    assert ret_val

    # test level 3, part 2. this should actually let the relay occur
    # but because of bogus relay host values, this will throw an exception and return false
    ret_val: bool = queue_utils.relay_msg(b'{"test": "test"}', True)

    # check the result
    assert not ret_val

    # replace the relay host config items
    os.environ['RELAY_RABBITMQ_HOST'] = 'localhost'

    # test level 3, part 1. disable the relay, call the method with force off
    os.environ['RELAY_ENABLED'] = 'True'

    # finally, post a msg to a test queue. this presumes that the MQ port has been forwarded locally
    # don't forget to remove the queue upon successful posting
    ret_val: bool = queue_utils.relay_msg(b'{"test": "test"}', True)

    # check the result
    assert ret_val

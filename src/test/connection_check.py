# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    checks a connection to a message queue

    Author: Phil Owen, RENCI.org
"""
import sys
import argparse
from src.common.queue_utils import QueueUtils


if __name__ == '__main__':
    # main entry point for the rule run.

    # create a command line parser
    parser = argparse.ArgumentParser(description='help', formatter_class=argparse.RawDescriptionHelpFormatter)

    # assign the expected input args
    parser.add_argument('-q', '--queue', help='the name of the queue that will be posted to.')

    # parse the command line
    args = parser.parse_args()

    queue_utils = QueueUtils(args.queue)

    # send the msg to the queue specified
    success = queue_utils.relay_msg('test message', True)

    # check the result
    assert success

    # exit with pass/fail
    sys.exit(0)

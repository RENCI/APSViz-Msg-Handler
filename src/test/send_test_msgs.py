# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Methods to handle ASGS database activity

    Author: Phil Owen, RENCI.org
"""
import sys
import time
import argparse
from src.common.queue_utils import QueueUtils


if __name__ == '__main__':
    # main entry point for the rule run.

    # create a command line parser
    parser = argparse.ArgumentParser(description='help', formatter_class=argparse.RawDescriptionHelpFormatter)

    # assign the expected input args
    parser.add_argument('-f', '--filename', help='Input file name that contains test queue data')
    parser.add_argument('-q', '--queue', help='the name of the queue that will be posted to.')
    parser.add_argument('-d', '--delay', help='the amount of time to pause (in seconds) between queue submissions.', default=2)

    # parse the command line
    args = parser.parse_args()

    queue_utils = QueueUtils(args.queue)

    # open messages file
    with open(args.filename, 'r', encoding='utf-8') as f:
        # submit each line in the file to the queue
        for line in f:
            # the data is in json/text format
            msg_obj: str = line

            # send the msg to the queue specified
            queue_utils.relay_msg(str.encode(msg_obj), True)

            # something for the user
            print(msg_obj)

            # pause between each queue submission
            time.sleep(int(args.delay))

    # exit with pass/fail
    sys.exit(0)

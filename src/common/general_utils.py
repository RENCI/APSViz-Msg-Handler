# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    General Utils - Various utilities common to this project's components.

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""

import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from src.common.logger import LoggingUtil


class GeneralUtils:
    """
    Utility methods used for components in this project.
    """
    def __init__(self, _logger=None):
        """
        Initializes this class

        """
        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("APSVIZ.Archiver.GeneralUtils", level=log_level, line_format='medium', log_file_path=log_path)

        # init the Slack channels
        self.slack_channels: dict = {'slack_status_channel': os.getenv('SLACK_STATUS_CHANNEL'),
                                     'slack_issues_channel': os.getenv('SLACK_ISSUES_CHANNEL')}

        # get the environment this instance is running on
        self.system = os.getenv('SYSTEM', 'System name not set')

    def send_slack_msg(self, msg, channel, debug_mode=False):
        """
        sends a msg to the Slack channel

        :param msg: the msg to be sent
        :param channel: the Slack channel to post the message to
        :param debug_mode: mode to indicate that this is a no-op
        :return: nothing
        """
        # init the final msg
        final_msg = f"APSViz Msg Handler ({self.system}) - {msg}"

        # log the message
        self.logger.info(final_msg)

        # send the message to Slack if not in debug mode and not running locally
        if not debug_mode and self.system in ['Dev', 'Prod', 'AWS/EKS']:
            # determine the client based on the channel
            if channel == 'slack_status_channel':
                client = WebClient(token=os.getenv('SLACK_STATUS_TOKEN'))
            else:
                client = WebClient(token=os.getenv('SLACK_ISSUES_TOKEN'))

            try:
                # send the message
                client.chat_postMessage(channel=self.slack_channels[channel], text=final_msg)
            except SlackApiError:
                # log the error
                self.logger.exception('Slack %s messaging failed. msg: %s', self.slack_channels[channel], final_msg)

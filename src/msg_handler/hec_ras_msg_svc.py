# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Entrypoint for the HEC/RAS msg svc queue listener/handler

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
import os

from src.common.logger import LoggingUtil
from src.common.queue_callbacks import QueueCallbacks
from src.common.queue_utils import QueueUtils


def run():
    """
    Fires up the hec/ras run properties message listener/handler

    :return:
    """
    # get the log level and directory from the environment.
    log_level, log_path = LoggingUtil.prep_for_logging()

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.Msg-Handler.hec_ras_msg_svc", level=log_level, line_format='medium',
                                      log_file_path=log_path)

    # set the app version
    app_version = os.getenv('APP_VERSION', 'Version number not set')

    logger.info("Initializing hec_ras_msg_svc handler, version: %s.", app_version)

    try:
        # get the queue name
        queue_name: str = os.getenv('HECRAS_RP_QUEUE_NAME', None)

        # did we get a queue name
        if queue_name is not None:
            # get a reference to the callback handler
            queue_callback = QueueCallbacks(_queue_name=queue_name, _logger=logger)

            # get a reference to the common queue utilities
            queue_utils = QueueUtils(_queue_name=queue_name, _logger=logger)

            # start consuming the messages
            queue_utils.start_consuming(queue_callback.hecras_run_props_callback)
        else:
            logger.error('FAILURE - HECRAS run property queue name not specified. Queue handling not started.')

    except Exception:
        logger.exception("FAILURE - Problems initiating hec_ras_msg_svc.")


if __name__ == "__main__":
    run()

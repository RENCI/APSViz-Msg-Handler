# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Entrypoint for the ecflow_rp run properties msg svc queue listener/handler

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
from src.common.logger import LoggingUtil
from src.common.asgs_queue_callback import AsgsQueueCallback
from src.common.queue_utils import QueueUtils


def run():
    """
    Fires up the ecflow_rp run properties message listener/handler

    :return:
    """
    # get the log level and directory from the environment.
    log_level, log_path = LoggingUtil.prep_for_logging()

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.APSViz-Msg-Handler.ecflow_rp_run_props_msg_svc", level=log_level, line_format='medium',
                                      log_file_path=log_path)

    logger.info("Initializing ecflow_rp_run_props_msg_svc handler.")

    try:
        # get a reference to the common callback handler
        queue_callback = AsgsQueueCallback(_queue_name='rp_queue', _logger=logger)

        # get a reference to the common queue utilities
        queue_utils = QueueUtils(_queue_name='rp_queue', _logger=logger)

        # start consuming the messages
        queue_utils.start_consuming(queue_callback.ecflow_rp_run_props_callback)

    except Exception:
        logger.exception("FAILURE - Problems initiating ecflow_rp_run_props_msg_svc.")


if __name__ == "__main__":
    run()

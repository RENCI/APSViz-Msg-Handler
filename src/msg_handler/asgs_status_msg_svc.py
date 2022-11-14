# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Entrypoint for the ASGS status queue message listener/handler

    Authors: Lisa Stillwell, Phil Owen @RENCI.org
"""
from src.common.logger import LoggingUtil
from src.common.asgs_queue_callback import AsgsQueueCallback


def run():
    """
    Fires up the ASGS status queue message listener/handler

    :return:
    """
    # get the log level and directory from the environment.
    log_level, log_path = LoggingUtil.prep_for_logging()

    # create a logger
    logger = LoggingUtil.init_logging("APSVIZ.APSViz-Msg-Handler.asgs_status_msg_svc", level=log_level, line_format='medium', log_file_path=log_path)

    logger.info("Initializing asgs_status_msg_svc handler.")

    try:
        # get an instance to the callback handler
        queue_callback_inst = AsgsQueueCallback(_queue_name='asgs_queue', _logger=logger)

        # start consuming the messages
        queue_callback_inst.start_consuming(queue_callback_inst.asgs_msg_callback)

    except Exception:
        logger.exception("FAILURE - Problems initializing asgs_status_msg_svc.")


if __name__ == "__main__":
    run()

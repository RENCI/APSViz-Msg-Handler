#!/bin/bash

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

echo "Sleeping $1 second(s) to give the RabbitMQ time to initialize"
sleep "$1"
echo "Starting message handlers..."
python src/msg_handler/receive_msg_service_pg.py &
python src/msg_handler/receive_cfg_msg_service_pg.py &
while true; do sleep 600; done;

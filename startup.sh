#!/bin/bash

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

echo "Sleeping $1 second(s) to give the RabbitMQ time to initialize"
sleep "$1"
echo "Starting message handlers..."
python src/msg_handler/asgs_status_msg_svc.py &
python src/msg_handler/ecflow_run_props_msg_svc.py &
python src/msg_handler/ecflow_run_time_msg_svc.py &
python src/msg_handler/asgs_run_props_msg_svc.py &
python src/msg_handler/hec_ras_msg_svc.py &
while true; do sleep 3600; done;

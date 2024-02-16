<!--
SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.

SPDX-License-Identifier: GPL-3.0-or-later
SPDX-License-Identifier: LicenseRef-RENCI
SPDX-License-Identifier: MIT
-->

![image not found](renci-logo.png "RENCI")

# APSViz RabbitMQ Message Handler
RabbitMQ message handlers for the APSViz project, including handlers for:
 - The ECFLOW run-time status and run properties queues
 - The ECFLOW run-time status and run properties queues
 - The HEC/RAS queue

#### Licenses...
[![MIT License](https://img.shields.io/badge/License-MIT-orange.svg)](https://github.com/RENCI/APSViz-Msg-Handler/tree/master/LICENSE)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![RENCI License](https://img.shields.io/badge/License-RENCI-blue.svg)](https://www.renci.org/)
#### Components and versions...
[![Python](https://img.shields.io/badge/Python-3.12.2-orange)](https://github.com/python/cpython)
[![Linting Pylint](https://img.shields.io/badge/Pylint-%203.0.3-yellow)](https://github.com/PyCQA/pylint)
[![Pytest](https://img.shields.io/badge/Pytest-%208.0.0-blue)](https://github.com/pytest-dev/pytest)
#### Build status...
[![Pylint](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/pylint.yml/badge.svg)](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/pylint.yml)
[![Build and push the Docker image](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/image-push.yml/badge.svg)](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/image-push.yml)

## Description
This product is designed to process messages that appear on the APSViz RabbitMQ server.

There are GitHub actions to maintain code quality in this repo:
 - Pylint (minimum score of 10/10 to pass),
 - Build/publish a Docker image.

Helm/k8s charts for this product are available at: [APSViz-Helm](https://github.com/RENCI/apsviz-helm/tree/main/apsviz-msg-handler).

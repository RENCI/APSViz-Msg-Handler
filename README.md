<!--
SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.

SPDX-License-Identifier: GPL-3.0-or-later
SPDX-License-Identifier: LicenseRef-RENCI
SPDX-License-Identifier: MIT
-->

![image not found](renci-logo.png "RENCI")

# APSViz-m
RabbitMQ message handlers for the APSViz project.

#### Licenses...
[![MIT License](https://img.shields.io/badge/License-MIT-red.svg)](https://github.com/RENCI/APSViz-Msg-Handler/blob/master/LICENSE)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![RENCI License](https://img.shields.io/badge/License-RENCI-blue.svg)](https://renci.org/)
#### Components and versions...
[![Python](https://img.shields.io/badge/Python-3.10.8-orange)](https://github.com/PyCQA/pylint)
[![Linting Pylint](https://img.shields.io/badge/Pylint-%202.15.5-yellowgreen)](https://github.com/PyCQA/pylint)
#### Build status...
[![Pylint](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/pylint.yml/badge.svg)](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/pylint.yml)
[![Build and push the Docker image](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/image-push.yml/badge.svg)](https://github.com/RENCI/APSViz-Msg-Handler/actions/workflows/image-push.yml)

## Description
This product is designed to process messages that appear on the APSViz RabbitMQ server.

There are GitHub actions to maintain code quality in this repo:
 - Pylint (minimum score of 10/10 to pass),
 - Build/publish a Docker image.

Helm/k8s charts for this product are available at: [APSViz-Helm](https://github.com/RENCI/apsviz-helm/tree/main/apsviz-msg-handler).

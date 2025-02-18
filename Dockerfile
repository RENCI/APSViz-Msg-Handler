# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

# This Dockerfile is used to build THE RabbitMQ message handler python image

FROM python:3.13.2-slim

# install basic tools
RUN apt-get update && apt-get -y upgrade && apt-get install -yq vim procps

# update pip
RUN pip install --upgrade pip

# clear out the apt cache
RUN apt-get clean

# get some credit
LABEL maintainer="powen@renci.org"

# get the build argument that has the version
ARG APP_VERSION=$(APP_VERSION)

# now add the version arg value into a ENV param
ENV APP_VERSION=$APP_VERSION

# go to the directory where we are going to upload the repo
WORKDIR /repo/message

# install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# get all queue message handler files into this image
COPY src src
COPY ./startup.sh ./startup.sh

# make sure the file has execute permissions
RUN chmod 777 -R /repo/message

# create a new non-root user and switch to it
RUN useradd --create-home -u 1000 nru
USER nru

# set the python path
ENV PYTHONPATH=/repo/message

# start the services
ENTRYPOINT ["bash", "startup.sh", "30"]

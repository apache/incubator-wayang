# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG WAYANG_IMAGE=apache-wayang:ci
FROM ${WAYANG_IMAGE} AS wayang-runtime

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV WAYANG_API_HOST=wayang-rest
ENV WAYANG_API_PORT=8080
ENV FLAG_WAYANG=true
ENV JAVA_HOME=/opt/java/openjdk
ENV WAYANG_HOME=/opt/wayang
ENV PATH=/opt/wayang/bin:/opt/java/openjdk/bin:/opt/wayang/python/.venv/bin:${PATH}

USER root

COPY --from=wayang-runtime /opt/java/openjdk /opt/java/openjdk
COPY --from=wayang-runtime /opt/wayang /opt/wayang

WORKDIR /opt/wayang/python

COPY python/ /opt/wayang/python/

RUN mkdir -p /root/.wayang \
    && python3 -m venv /opt/wayang/python/.venv \
    && python -m pip install --no-cache-dir -r src/pywy/requirements.txt \
    && python -m pip install --no-cache-dir . \
    && printf '%s\n' \
        'wayang.api.python.worker = /opt/wayang/python/src/pywy/execution/worker.py' \
        'wayang.api.python.path = /opt/wayang/python/.venv/bin/python' \
        'wayang.api.python.env.path = /opt/wayang/python/src' \
        >> /opt/wayang/conf/wayang.properties

COPY python/docker/entrypoint.sh /usr/local/bin/wayang-python-entrypoint

RUN chmod +x /usr/local/bin/wayang-python-entrypoint

WORKDIR /opt/wayang

ENTRYPOINT ["bin/wayang-submit"]
CMD ["org.apache.wayang.api.json.Main", "8080"]

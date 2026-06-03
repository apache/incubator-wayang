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

FROM eclipse-temurin:17-jre

ARG WAYANG_DIST=wayang-assembly/target/*-dist.tar.gz

ENV WAYANG_HOME=/opt/wayang
ENV PATH="${WAYANG_HOME}/bin:${PATH}"

COPY ${WAYANG_DIST} /tmp/wayang-dist.tar.gz

RUN mkdir -p /opt /root/.wayang \
    && tar -xzf /tmp/wayang-dist.tar.gz -C /opt \
    && extracted_dir="$(find /opt -mindepth 1 -maxdepth 1 -type d -name 'wayang-*' | head -n 1)" \
    && test -n "${extracted_dir}" \
    && ln -s "${extracted_dir}" "${WAYANG_HOME}" \
    && rm /tmp/wayang-dist.tar.gz

WORKDIR /opt/wayang

ENTRYPOINT ["/opt/wayang/bin/wayang-submit"]
CMD ["org.apache.wayang.apps.pi.PiEstimation", "java", "1"]

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

FROM eclipse-temurin:17-jre AS wayang-assembler

ARG WAYANG_DIST=wayang-assembly/target/*-dist.tar.gz

COPY ${WAYANG_DIST} /tmp/wayang-dist.tar.gz

RUN mkdir -p /opt/wayang-root /opt/wayang /opt/wayang/smoke \
    && tar -xzf /tmp/wayang-dist.tar.gz -C /opt/wayang-root \
    && extracted_dir="$(find /opt/wayang-root -mindepth 1 -maxdepth 1 -type d -name 'wayang-*' | head -n 1)" \
    && test -n "${extracted_dir}" \
    && cp -a "${extracted_dir}/." /opt/wayang/ \
    && find /opt/wayang -type f \( -name "*-sources.jar" -o -name "*-javadoc.jar" -o -name "README.md" \) -delete \
    && printf "apache wayang docker smoke test\n" > /opt/wayang/smoke/wordcount.txt \
    && rm /tmp/wayang-dist.tar.gz

FROM eclipse-temurin:17-jre

ENV WAYANG_HOME=/opt/wayang
ENV FLAG_WAYANG=true
ENV PATH="${WAYANG_HOME}/bin:${PATH}"

COPY --from=wayang-assembler /opt/wayang /opt/wayang

RUN mkdir -p /root/.wayang /opt/wayang/conf \
    && touch /opt/wayang/conf/wayang.properties

WORKDIR /opt/wayang

ENTRYPOINT ["/opt/wayang/bin/wayang-submit"]
CMD ["org.apache.wayang.apps.pi.PiEstimation", "java", "1"]

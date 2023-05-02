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

# This file need to be modified as required
FROM node:lts-alpine as build-stage

RUN mkdir -p /frontend
WORKDIR /frontend

# copy both 'package.json' and 'package-lock.json' (if available)
# TODO: validate the folder
COPY /src/package*.json ./

# install project dependencies
RUN npm install

# copy project files and folders to the current working directory (i.e. 'app' folder)
# TODO: validate the folder
COPY /src ./

# build app for production with minification
WORKDIR /frontend
ENV NODE_OPTIONS=--max-old-space-size=8192
RUN npm run build

FROM nginx:stable-alpine as production-stage
COPY --from=build-stage /frontend/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
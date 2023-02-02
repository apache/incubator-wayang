/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.agoraeo.patches;

import java.io.Serializable;

public class L2a_file implements Serializable {

    String l2a_uuid;
    String l2a_path;
    String l1c_uuid;

    public L2a_file(String l2a_uuid, String l2a_path, String l1c_uuid) {
        this.l2a_uuid = l2a_uuid;
        this.l2a_path = l2a_path;
        this.l1c_uuid = l1c_uuid;
    }

    public String getL2a_uuid() {
        return l2a_uuid;
    }

    public void setL2a_uuid(String l2a_uuid) {
        this.l2a_uuid = l2a_uuid;
    }

    public String getL2a_path() {
        return l2a_path;
    }

    public void setL2a_path(String l2a_path) {
        this.l2a_path = l2a_path;
    }

    public String getL1c_uuid() {
        return l1c_uuid;
    }

    public void setL1c_uuid(String l1c_uuid) {
        this.l1c_uuid = l1c_uuid;
    }

    @Override
    public String toString() {
        return l2a_uuid + "|" + l2a_path + "|" + l1c_uuid;
    }
}

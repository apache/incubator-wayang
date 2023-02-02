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

public class Band implements Serializable {

    String l1c_uuid;
    String l2a_uuid;
    String l2a_path;
    String band_name;
    String band_path;
    String resolution;
    String utm;

    public Band(String l1c_uuid, String l2a_uuid, String l2a_path, String band_name, String band_path, String resolution, String utm) {
        this.l1c_uuid = l1c_uuid;
        this.l2a_uuid = l2a_uuid;
        this.l2a_path = l2a_path;
        this.band_name = band_name;
        this.band_path = band_path;
        this.resolution = resolution;
        this.utm = utm;
    }

    public String getL1c_uuid() {
        return l1c_uuid;
    }

    public void setL1c_uuid(String l1c_uuid) {
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

    public String getBand_name() {
        return band_name;
    }

    public void setBand_name(String band_name) {
        this.band_name = band_name;
    }

    public String getBand_path() {
        return band_path;
    }

    public void setBand_path(String band_path) {
        this.band_path = band_path;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getUtm() {
        return utm;
    }

    public void setUtm(String utm) {
        this.utm = utm;
    }

    @Override
    public String toString() {
        return this.band_name + "|band_path:" + this.band_path.substring(this.band_path.lastIndexOf("/")+1) + "|" + this.resolution + "|" + this.utm + "|l2a_path:" + this.l2a_path.substring(this.l2a_path.lastIndexOf("/")+1);
    }
}

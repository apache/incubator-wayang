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

package org.apache.wayang.apps.tpch.data;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A tuple of the region table.
 */
public class RegionTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public int R_REGIONKEY;

    /**
     * {@code string}
     */
    public String R_NAME;

    /**
     * {@code variable text, size 44}
     */
    public String R_COMMENT;

    public RegionTuple(int r_REGIONKEY,
                         String r_NAME,
                         String r_COMMENT) {
        this.R_REGIONKEY = r_REGIONKEY;
        this.R_NAME = r_NAME;
        this.R_COMMENT = r_COMMENT;
    }

    public RegionTuple() {
    }

    public static class Parser {

        public RegionTuple parse(String line, char delimiter) {
            RegionTuple tuple = new RegionTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.R_REGIONKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.R_NAME = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.R_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }
    }
}

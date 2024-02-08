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
 * A tuple of the nation table.
 */
public class NationTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public int N_NATIONKEY;

    /**
     * {@code string}
     */
    public String N_NAME;

    /**
     * {@code integer}, {@code FK}
     */
    public int N_REGIONKEY;

    /**
     * {@code variable text, size 44}
     */
    public String N_COMMENT;

    public NationTuple(int n_NATIONKEY,
                         String n_NAME,
                         int n_REGIONKEY,
                         String n_COMMENT) {
        this.N_NATIONKEY = n_NATIONKEY;
        this.N_NAME = n_NAME;
        this.N_REGIONKEY = n_REGIONKEY;
        this.N_COMMENT = n_COMMENT;
    }

    public NationTuple() {
    }

    public static class Parser {

        public NationTuple parse(String line, char delimiter) {
            NationTuple tuple = new NationTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.N_NATIONKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.N_NAME = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.N_REGIONKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.N_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }
    }
}

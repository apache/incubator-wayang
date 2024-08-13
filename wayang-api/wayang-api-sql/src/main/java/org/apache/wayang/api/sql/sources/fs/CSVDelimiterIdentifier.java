/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.sources.fs;

import java.util.HashMap;
import java.util.Map;

public class CSVDelimiterIdentifier {

    public static char identifyDelimiter(String data) {
        Map<Character, Integer> delimiterCounts = new HashMap<>();
        for (char c : new char[]{',', ';', '\t', '|', ' '}) {
            delimiterCounts.put(c, 0);
        }

        for (char c : data.toCharArray()) {
            if (delimiterCounts.containsKey(c)) {
                delimiterCounts.put(c, delimiterCounts.get(c) + 1);
            }
        }

        char maxDelimiter = ' ';
        int maxCount = 0;
        for (Map.Entry<Character, Integer> entry : delimiterCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxDelimiter = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        return maxDelimiter;
    }


}




/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.utils;

import java.util.ArrayList;
import java.util.List;

public class StringUtil {

    /**
     * Split a string with an unknown number of tokens with a given separator.
     *
     * @param string    the string to split
     * @param separator the separator
     * @return list of tokens obtained by splitting
     */
    public static List<String> split(String string, char separator) {
        final List<String> list = new ArrayList<>();
        int index = 0;
        int start = 0;
        final int len = string.length();
        while (index < len) {
            if (string.charAt(index) == separator) {
                list.add(string.substring(start, index));
                start = ++index;
            }
            else
                ++index;
        }
        if (start != index)
            list.add(string.substring(start, index));

        return list;
    }

    public static String[] split(String string, char separator, int limit) {
        final String[] result = new String[limit];
        int index = 0;
        int start = 0;
        final int len = string.length();
        int pos = 0;
        while (index < len) {
            if (string.charAt(index) == separator) {
                result[pos] = string.substring(start, index);
                ++pos;
                if (pos == limit) {
                    return result;
                }
                start = ++index;
            } else {
                ++index;
            }
        }
        if (start != index) {
            result[pos] = string.substring(start, index);
        }
        return result;
    }


}

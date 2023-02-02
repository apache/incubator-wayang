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

package org.apache.wayang.agoraeo.utilities;

import org.apache.wayang.agoraeo.sentinel.Mirror;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Utilities {

    //--from NOW-30DAY --to NOW --order 33UUU,32VNM
    //from -> NOW-30DAY; to -> NOW; order -> {33UU, 32VNM}
    // {from -> NOW-30DAY; to -> NOW; order -> 33UU};{from -> NOW-30DAY; to -> NOW; order -> 32VNM}
    public static List<Map<String, String>> flattenParameters(Map<String, List<String>> iterable, Map<String, String> statics){

        // 1 from, to
        // N orders
        // M mirrors
        int max_lenght = 0;

        for(Map.Entry<String, List<String>> elem : iterable.entrySet()){
            if(elem.getValue().size() > max_lenght)
                max_lenght = elem.getValue().size();
        }

        List<Map<String, String>> res = new ArrayList<>();

        for (int i =0; i< max_lenght; i++){
            Map<String, String> clone = cloneMap(statics);

            for(String key : iterable.keySet()){
                List<String> values = iterable.get(key);

                if (values.size()> i){
                    clone.put(key, values.get(i));
                }
            }

            res.add(clone);
        }

        return res;
    }

    private static Map<String, String> cloneMap(Map<String, String> in) {
        Map<String, String> res = new HashMap<>();

        for (Map.Entry<String, String> el:in.entrySet()) {
            res.put(el.getKey(), el.getValue());
        }

        return res;
    }

    // assign url, user and password from mirror
    public static List<Map<String, String>> distribute(List<Map<String, String>> origin, String key, List<Mirror> distribute){
        return origin.stream()
                .map(
                        new Function<Map<String, String>, Map<String, String>>() {

                            int position = 0;
                            @Override
                            public Map<String, String> apply(Map<String, String> stringStringMap) {
                                if(position > distribute.size()){
                                    position = 0;
                                }
                                stringStringMap.put(key, distribute.get(position).getUrl());
                                stringStringMap.put("user", distribute.get(position).getUser());
                                stringStringMap.put("password", distribute.get(position).getPassword());
                                position++;
                                return stringStringMap;
                            }
                        }
                )
                .collect(Collectors.toList());
    }


    public static void main(String[] args) {
        //from -> NOW-30DAY; to -> NOW; order -> {33UU, 32VNM}
        Map<String, String> fecha = new HashMap<>();
        fecha.put("from", "NOW-30DAY");
        fecha.put("to", "NOW");

        Map<String, List<String>> pp = new HashMap<>();

        pp.put("order", Arrays.asList("33UUU", "32VNM"));
        pp.put("mirrors", Arrays.asList("https://sentinels.space.noa.gr/dhus", "https://scihub.copernicus.eu/dhus"));

        System.out.println(flattenParameters(pp, fecha));
    }

}

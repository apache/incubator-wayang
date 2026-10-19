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

package org.apache.wayang.ml.util;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.json.WayangJsonObj;

public class CardinalitySampler {

    public static List<SampledCardinality> samples = new ArrayList<>();

    public static void configureWriteToFile(
            final Configuration config,
            final String filePath){
        config.setProperty("wayang.core.log.enabled", "true");
        config.setProperty("wayang.core.log.cardinalities", filePath);
        config.setProperty("wayang.core.optimizer.instrumentation", "org.apache.wayang.core.profiling.FullInstrumentationStrategy");

        // clear previous measurements from file
        try {
            final File f = new File(filePath);
            if(f.exists() && !f.isDirectory()) {
               new FileWriter(filePath, false).close();
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public static void readFromFile(final String filePath) {
        try {
            final SampledCardinality.Serializer serializer = new SampledCardinality.Serializer();
            samples = Files.lines(Path.of(filePath), Charset.forName("UTF-8"))
                .map(line -> {
                    try {
                        return JsonSerializables.deserialize(new WayangJsonObj(line), serializer, SampledCardinality.class);
                    } catch (final Exception e) {
                        System.out.println("Exception: " + e);
                        throw new WayangException(String.format("Could not parse \"%s\".", new WayangJsonObj(line).getNode()), e);
                    }
                }).collect(Collectors.toList());
        } catch(final Exception e) {
            e.printStackTrace();
        }
    }
}

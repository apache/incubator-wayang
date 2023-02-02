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

package org.apache.wayang.agoraeo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.wayang.agoraeo.operators.basic.Sen2CorWrapper;
import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.agoraeo.sentinel.Mirror;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;

public class Main {
    public static void main(String[] args) {

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        wayangContext.register(WayangAgoraEO.plugin());

        Configuration config = wayangContext.getConfiguration();
        config.load(ReflectionUtils.loadResource(WayangAgoraEO.DEFAULT_CONFIG_FILE));

        String sen2cor = config.getStringProperty("org.apache.wayang.agoraeo.sen2cor.location");
        String l2a_images_folder = config.getStringProperty("org.apache.wayang.agoraeo.images.l2a");

        System.out.println("Running AgoraEO!");

        // TODO: Read all from config file
        Mirror m1 = new Mirror("https://scihub.copernicus.eu/dhus", "rpardomeza", "12c124ccb2");
        Mirror m2 = new Mirror("https://colhub.met.no", "rpardomeza", "12c124ccb2");

        List<Mirror> mirrors = Arrays.asList(m1,m2);

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        String sen2corlog = now.format(dtf);

        WayangPlan w = alternative2WayangPlan(
                mirrors,
                sen2cor,
                l2a_images_folder,
                "file:///Users/rodrigopardomeza/tu-berlin/agoraeo/agoraeo/sen2cor_logs/"+sen2corlog
        );

        wayangContext.execute(w, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class), ReflectionUtils.getDeclaringJar(SparkPlatform.class));

    }

    public static WayangPlan alternative2WayangPlan(
            List<Mirror> mirrors,
            String sen2cor,
            String l2a_location,
            String outputFileUrl
    ) {

        SentinelSource source = new SentinelSource()
            .setFrom("NOW-30DAY")
            .setTo("NOW")
            .setOrder(Arrays.asList("33UWP", "32VNM"))
            .setMirrors(mirrors)
        ;

        Sen2CorWrapper toL2A = new Sen2CorWrapper(sen2cor, l2a_location);

        /* TODO: BigEarthNet Pipeline */

        TextFileSink<String> sink = new TextFileSink<>(outputFileUrl, String.class);

        source.connectTo(0,toL2A,0);
        toL2A.connectTo(0,sink,0);


        return new WayangPlan(sink);
    }

}
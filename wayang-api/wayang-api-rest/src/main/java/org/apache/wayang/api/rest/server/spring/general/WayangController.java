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

package org.apache.wayang.api.rest.server.spring.general;

import org.apache.wayang.api.python.function.WrappedPythonFunction;
import org.apache.wayang.basic.operators.MapPartitionsOperator;
import org.apache.wayang.basic.operators.TextFileSink;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.commons.serializable.OperatorProto;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.types.DataSetType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

import org.apache.wayang.commons.serializable.WayangPlanProto;


@RestController
public class WayangController {

    @GetMapping("/")
    public String all(){
        System.out.println("detected!");

        try {

            //FileInputStream inputStream = new FileInputStream("/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/message");
            FileInputStream inputStream = new FileInputStream("/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/filter_message");
            WayangPlanProto plan = WayangPlanProto.parseFrom(inputStream);

            WayangContext wc = buildContext(plan);
            WayangPlan wp = buildPlan(plan);

            wc.execute(wp);
            return("Works!");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "Not working";
    }

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

    private WayangContext buildContext(WayangPlanProto plan) {
        WayangContext ctx = new WayangContext();
            /*for(Pywayangplan.Context.Platform platform : plan.getContext().getPlatformsList()){
                if (platform.getNumber() == 0)
                    ctx.with(Java.basicPlugin());
                else if(platform.getNumber() == 1)
                    ctx.with(Spark.basicPlugin());
            }*/

        plan.getContext().getPlatformsList().forEach(platform -> {
            if (platform.getNumber() == 0)
                ctx.with(Java.basicPlugin());
            else if (platform.getNumber() == 1)
                ctx.with(Spark.basicPlugin());
        });

        return ctx;
    }

    private WayangPlan buildPlan(WayangPlanProto plan) {

        try {

            String sourcepath = plan.getPlan().getSource().getPath();
            URL url = new File(sourcepath).toURI().toURL();
            System.out.println("sourceurl");
            System.out.println(url.toString());
            TextFileSource textFileSource = new TextFileSource(url.toString());

            DataSetType d = null;
            Class t = null;
            switch (plan.getPlan().getOutput().getNumber()){
                case 0:
                    d = DataSetType.createDefault(String.class);
                    t = String.class;
                    break;
                case 1:
                    d = DataSetType.createDefault(Integer.class);
                    t = Integer.class;
                    break;
            }
            String sinkpath = plan.getPlan().getSink().getPath();
            URL sink_url = new File(sinkpath).toURI().toURL();
            System.out.println("sink");
            System.out.println(sink_url.toString());
            TextFileSink textFileSink = new TextFileSink(
                    sink_url.toString(),
                    t
            );

            connectOperators(textFileSource, textFileSink, plan.getPlan().getOperatorsList());
            /** There is only one operator
            OperatorProto op = plan.getPlan().getOperators(0);
            MapPartitionsOperator<String, String> filter =
                    new MapPartitionsOperator<>(
                            new MapPartitionsDescriptor<String, String>(
                                    new WrappedPythonFunction<String, String>(
                                            l -> l,
                                            op.getUdf()
                                    ),
                                    String.class,
                                    String.class
                            )
                    );

            textFileSource.connectTo(0, filter, 0);
            filter.connectTo(0, textFileSink, 0);*/
            return new WayangPlan(textFileSink);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        throw new WayangException("Unable to create Plan");
    }

    private void connectOperators(
            TextFileSource textFileSource,
            TextFileSink textFileSink,
            List<OperatorProto> operatorsList) {

        MapPartitionsOperator<String, String> current = null;
        OperatorBase previous = null;

        if(operatorsList.size() < 1){
            textFileSource.connectTo(0, textFileSink, 0);
            return;
        }

        /* TODO operatorList should be a list of pipelines*/
        /* This just describe the operation in one pipeline*/
        for(OperatorProto op : operatorsList){

            if(current == null)
                previous = textFileSource;
            else
                previous = current;

            current =
                    new MapPartitionsOperator<>(
                            new MapPartitionsDescriptor<String, String>(
                                    new WrappedPythonFunction<String, String>(
                                            l -> l,
                                            op.getUdf()
                                    ),
                                    String.class,
                                    String.class
                            )
                    );

            previous.connectTo(0, current, 0);
        }

        current.connectTo(0, textFileSink, 0);

        return;
    }
}

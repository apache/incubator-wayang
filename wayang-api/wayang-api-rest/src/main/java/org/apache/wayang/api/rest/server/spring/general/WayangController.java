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
import org.apache.wayang.basic.operators.UnionAllOperator;
import org.apache.wayang.commons.serializable.OperatorProto;
import org.apache.wayang.commons.serializable.PlanProto;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
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
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
            FileInputStream inputStream = new FileInputStream(Paths.get(".").toRealPath() + "/protobuf/wayang_message");
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

    private WayangContext buildContext(WayangPlanProto plan){

        WayangContext ctx = new WayangContext();
        plan.getContext().getPlatformsList().forEach(platform -> {
            if (platform.getNumber() == 0)
                ctx.with(Java.basicPlugin());
            else if (platform.getNumber() == 1)
                ctx.with(Spark.basicPlugin());
        });

        return ctx;
    }

    private WayangPlan buildPlan(WayangPlanProto plan){

        System.out.println(plan);

        PlanProto planProto = plan.getPlan();
        LinkedList<OperatorProto> protoList = new LinkedList<>();
        planProto.getSourcesList().forEach(protoList::addLast);

        Map<String, OperatorBase> operators = new HashMap<>();
        List<OperatorBase> sinks = new ArrayList<>();
        while(! protoList.isEmpty()) {

            OperatorProto proto = protoList.pollFirst();

            /* Checking if protoOperator can be connected to the current WayangPlan*/
            boolean processIt;
            if(proto.getType().equals("source")) processIt = true;

            else {
                /* Checking if ALL predecessors were already processed */
                processIt = true;
                for(String predecessor : proto.getPredecessorsList()){
                    if (!operators.containsKey(predecessor)) {
                        processIt = false;
                        break;
                    }
                }
            }

            /* Operators should not be processed twice*/
            if(operators.containsKey(proto.getId())) processIt = false;

            if(processIt) {

                /* Create and store Wayang operator */
                OperatorBase operator = createOperatorByType(proto);
                operators.put(proto.getId(), operator);

                /*TODO Connect with predecessors requires more details in connection slot*/
                int order = 0;
                for (String pre_id : proto.getPredecessorsList()) {

                    OperatorBase predecessor = operators.get(pre_id);
                    /* Only works without replicate topology */
                    predecessor.connectTo(0, operator, order);
                    order++;

                    if(proto.getType().equals("sink")){
                        sinks.add(operator);
                        //if(!sinks.contains(operator)) {
                        //    sinks.add(operator);
                        //}
                    }
                }

                /*List of OperatorProto successors
                 * They will be added to the protoList
                 * nevertheless they must be processed only if the parents are in operators list */
                List<OperatorProto> listSuccessors = planProto.getOperatorsList()
                        .stream()
                        .filter(t -> proto.getSuccessorsList().contains(t.getId()))
                        .collect(Collectors.toList());
                for (OperatorProto successor : listSuccessors){
                    if(!protoList.contains(successor)){
                        protoList.addLast(successor);
                    }
                }

                List<OperatorProto> sinkSuccessors = planProto.getSinksList()
                        .stream()
                        .filter(t -> proto.getSuccessorsList().contains(t.getId()))
                        .collect(Collectors.toList());
                for (OperatorProto successor : sinkSuccessors){
                    if(!protoList.contains(successor)){
                        protoList.addLast(successor);
                    }
                }

            } else {

                /* In case we cannot process it yet, It must be added again at the end*/
                protoList.addLast(proto);
            }
        }

        WayangPlan wayangPlan = new WayangPlan(sinks.get(0));
        return wayangPlan;
    }

    public OperatorBase createOperatorByType(OperatorProto operator){

        System.out.println("Typo: " + operator.getType());
        switch(operator.getType()){
            case "source":
                try {
                    String source_path = operator.getPath();
                    URL url = new File(source_path).toURI().toURL();
                    return new TextFileSource(url.toString());
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                break;
            case "sink":
                try {
                    String sink_path = operator.getPath();
                    URL url = new File(sink_path).toURI().toURL();
                    return new TextFileSink<String>(
                            url.toString(),
                            String.class
                    );

                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
                break;
            case "map_partition":
                return new MapPartitionsOperator<>(
                    new MapPartitionsDescriptor<String, String>(
                        new WrappedPythonFunction<String, String>(
                            l -> l,
                            operator.getUdf()
                        ),
                        String.class,
                        String.class
                    )
                );

            case "union":
                return new UnionAllOperator<String>(
                        String.class
                );

        }

        throw new WayangException("Operator Type not supported");
    }

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

}

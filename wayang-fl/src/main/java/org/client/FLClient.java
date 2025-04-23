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

package org.client;


import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.OneForOneStrategy;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.SupervisorStrategy;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;

import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.functions.PlanFunction;
import org.messages.*;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

public class FLClient extends AbstractActor {

    private final Client client;
    private WayangPlan plan;
    private Collection<?> collector;
    private WayangContext wayangContext;
    private JavaPlanBuilder planBuilder;
    private PlanFunction planFunction;
    private Map<String, Object> hyperparams;

    private Plugin getPlugin(String platformType){
        if(platformType.equals("java")) return Java.basicPlugin();
        else if(platformType.equals("spark")) return Spark.basicPlugin();
        else return null;
    }

    public Props props(Client client, String platformType) {
        return Props.create(FLClient.class, () -> new FLClient(client, platformType));
    }

    public FLClient(Client client, String platformType) {
        this.client = client;
        this.wayangContext = new WayangContext(new Configuration()).withPlugin(getPlugin(platformType));
        this.planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(client.getName()+"-job")
                .withUdfJarOf(FLClient.class);
        this.collector = new LinkedList<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HandshakeMessage.class, this::handleHandshakeMessage)
                .match(PlanHyperparametersMessage.class, this::handlePlanHyperparametersMessage)
                .match(ClientUpdateRequestMessage.class, this::handleClientUpdateRequestMessage)
                .build();
    }

    private void handleHandshakeMessage(HandshakeMessage msg) {
        getSender().tell(new HandshakeAckMessage(), getSelf());
        System.out.println("sent handshake ack to server");
    }

    private void handlePlanHyperparametersMessage(PlanHyperparametersMessage msg) {
        System.out.println(client.getName() + " receiving plan");
        planFunction = msg.getSerializedplan();
        hyperparams = msg.getHyperparams();
        getSender().tell(new PlanHyperparametersAckMessage(), getSelf());
        System.out.println(client.getName() + " initialised plan function");
    }

    private void buildPlan(Object operand){
        System.out.println(hyperparams.get("inputFileUrl"));
        JavaPlanBuilder newPlanBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(client.getName()+"-job")
                .withUdfJarOf(FLClient.class);
        Operator op = planFunction.apply(operand, newPlanBuilder, hyperparams);
//        System.out.println(op);
        Class classType = op.getOutput(0).getType().getDataUnitType().getTypeClass();
        LocalCallbackSink<?> sink = LocalCallbackSink.createCollectingSink(collector, classType);
        op.connectTo(0, sink, 0);
        plan = new WayangPlan(sink);
        System.out.println(client.getName() + " built plan successfully");
        System.out.println(plan);
    }



    private void handleClientUpdateRequestMessage(ClientUpdateRequestMessage msg) {
        System.out.println(client.getName() + " Received compute request");
//        System.out.println(planFunction);
//        System.out.println(client.getName() + " Printed planFunction");
        Object operand = msg.getValue();
        buildPlan(operand);
        wayangContext.execute(client.getName() + "-job", plan);
        System.out.println(client.getName() + " executed plan successfully");
        getSender().tell(new ClientUpdateResponseMessage(new LinkedList<>(collector)), getSelf());
//        System.out.println(client.getName());
//        System.out.println(collector);
        collector.clear();
    }


}

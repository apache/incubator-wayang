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

package org.server;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSelection;
import org.apache.pekko.actor.Props;
import org.client.Client;
import org.components.aggregator.Aggregator;
import org.components.criterion.Criterion;
import org.functions.PlanFunction;
import org.messages.*;

import java.util.*;
import java.util.function.Function;

public class FLServer extends AbstractActor {

    private final Server server;
    private final Aggregator aggregator;
    private final Criterion criterion;
    private final Map<String, Object> hyperparams;
    private Object current_operand;
    private Map<String, Object> current_values;
    private final Map<String, Function<Object, Object>> update_rules;
    private final Function<Pair<Object, Object>, Object> update_operand;
    private Map<ActorRef, Boolean> active_clients = new HashMap<>();
    private List<Object> client_responses = new ArrayList<>();
    private int client_acks = 0;
    private int active_client_count = 0;
    private ActorRef jobRef;

    public static Props props(Server server, Aggregator aggregator,
                              Criterion criterion, Map<String, Object> hyperparams,
                              Object initial_operand, Map<String, Object> initial_values,
                              Function<Pair<Object, Object>, Object> update_operand, Map<String, Function<Object, Object>> update_rules) {
        return Props.create(FLServer.class, () -> new FLServer(server, aggregator, criterion, hyperparams, initial_operand, initial_values, update_operand, update_rules));
    }

    public FLServer(Server server, Aggregator aggregator,
                    Criterion criterion, Map<String, Object> hyperparams,
                    Object initial_operand, Map<String, Object> initial_values,
                    Function<Pair<Object, Object>, Object> update_operand, Map<String, Function<Object, Object>> update_rules){
        this.server = server;
        this.aggregator = aggregator;
        this.criterion = criterion;
        this.hyperparams = hyperparams;
        this.current_operand = initial_operand;
        this.current_values = initial_values;
        this.update_operand = update_operand;
        this.update_rules = update_rules;
    }

    public void handleInitiateHandshakeMessage(InitiateHandshakeMessage msg){
        List<Client> clients = msg.getClients();
        for(Client client : clients){
            ActorSelection client_selection = getContext().actorSelection(client.getUrl());
            client_selection.tell(new HandshakeMessage(), getSelf());
            System.out.println("Sent handshake to client");
        }
    }

    public void handleSendPlanHyperParametersMessage(SendPlanHyperparametersMessage msg){
        PlanFunction plan = msg.getPlan();
        Map<String, Object> client_hyperparams = msg.getClient_hyperparams();
        for(ActorRef client : active_clients.keySet()){
            if(!active_clients.get(client)) continue;
            active_client_count++;
            // remove this line later
            client_hyperparams.put("inputFileUrl", "file:/Users/vedantaneogi/Downloads/higgs_part"+active_client_count+".txt");
            client.tell(new PlanHyperparametersMessage(plan, client_hyperparams), getSelf());
        }
//        while(client_acks < active_client_count){}
        jobRef = getSender();
    }

    private void handlePlanHyperparametersAckMessage(PlanHyperparametersAckMessage msg){
        System.out.println("Received plan hyperparemeters ack");
        client_acks++;
        if(client_acks == active_client_count) jobRef.tell("DONE", getSelf());
    }

    public void handleRunIterationMessage(RunIterationMessage msg){
        client_responses.clear();
        for(ActorRef client : active_clients.keySet()){
            if(!active_clients.get(client)) continue;
            client.tell(new ClientUpdateRequestMessage(current_operand), getSelf());
        }
    }

    public void handleAggregateResponsesMessage(AggregateResponsesMessage msg){
        System.out.println("Iteration Over, Aggregating Responses");
        Object aggregatedResult = aggregator.aggregate(client_responses, hyperparams);
        getSender().tell(aggregatedResult, getSelf());
    }

    public void handleUpdateOperand(UpdateStateMessage msg){
        Object aggregatedResult = msg.getAggregatedResult();
        current_operand = update_operand.apply(Pair.of(current_operand, aggregatedResult));
        handleUpdateValues();
    }

    public void handleUpdateValues(){
        Set<String> keys = current_values.keySet();
        for(String key : keys){
            current_values.put(key, update_rules.get(key).apply(current_values.get(key)));
        }
    }

    public void handleCheckCriterionMessage(CheckCriterionMessage msg){
       Boolean evaluation =  criterion.check(current_values);
       getSender().tell(evaluation, getSelf());
    }

    public void handleFinalOperandMessage(FinalOperandMessage msg){
        getSender().tell(current_operand, getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HandshakeAckMessage.class, this::handleHandshakeAckMessage)
                .match(ClientUpdateResponseMessage.class, this::handleClientUpdateResponseMessage)
                .match(InitiateHandshakeMessage.class, this::handleInitiateHandshakeMessage)
                .match(SendPlanHyperparametersMessage.class, this::handleSendPlanHyperParametersMessage)
                .match(CheckCriterionMessage.class, this::handleCheckCriterionMessage)
                .match(RunIterationMessage.class, this::handleRunIterationMessage)
                .match(AggregateResponsesMessage.class, this::handleAggregateResponsesMessage)
                .match(UpdateStateMessage.class, this::handleUpdateOperand)
                .match(FinalOperandMessage.class, this::handleFinalOperandMessage)
                .match(PlanHyperparametersAckMessage.class, this::handlePlanHyperparametersAckMessage)
                .build();
    }

    private void handleHandshakeAckMessage(HandshakeAckMessage msg){
        active_clients.put(getSender(), true);
    }

    private void handleClientUpdateResponseMessage(ClientUpdateResponseMessage msg){
        client_responses.add(msg.getResult());
    }
}

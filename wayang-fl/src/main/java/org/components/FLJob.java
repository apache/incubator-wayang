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

package org.components;

import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pekko.actor.*;
import org.client.Client;
import org.components.aggregator.Aggregator;
import org.components.criterion.Criterion;
import org.components.hyperparameters.Hyperparameters;
import org.functions.PlanFunction;
import org.messages.*;
import org.server.FLServer;
import org.server.Server;

import org.apache.pekko.actor.Props;

import java.util.List;

import org.apache.pekko.pattern.Patterns;
import scala.concurrent.Future;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;


public class FLJob {
    private String job_id;
    private final String job_name;
    private List<Client> clients = new ArrayList<>();
    private ActorRef FLServerActor;
    private final Aggregator aggregator;
    private final Criterion criterion;
    private final PlanFunction plan;
    private final Hyperparameters hyperparameters;
    private final Object initial_operand;
    private final Map<String, Object> initial_values;
    private final Map<String, Function<Object, Object>> update_rules;
    private final Function<Pair<Object, Object>, Object> update_operand;

    public void setJobId(String job_id){
        this.job_id = job_id;
    }

    public void startFLServer(Server server, Config config){
        ActorSystem system = ActorSystem.create(job_name + "-system", config);
        this.FLServerActor = system.actorOf(
                Props.create(FLServer.class, () -> new FLServer(
                        server,
                        aggregator,
                        criterion,
                        hyperparameters.get_all_server_hyperparams(),
                        initial_operand,
                        initial_values,
                        update_operand,
                        update_rules
                )),
                server.getName()
        );
    }

    public FLJob(String job_name,
                 List<String> client_names, List<String> client_urls,
                 PlanFunction plan, Hyperparameters hyperparameters,
                 Criterion criterion, Aggregator aggregator,
                 Map<String, Object> initial_values, Object initial_operand,
                 Map<String, Function<Object, Object>> update_rules,
                 Function<Pair<Object,Object>, Object> update_operand){
        this.job_name = job_name;
        for(int i = 0; i < client_names.size(); i++){
            clients.add(new Client(client_urls.get(i), client_names.get(i)));
        }
        this.aggregator = aggregator;
        this.criterion = criterion;
        this.update_operand = update_operand;
        this.update_rules = update_rules;
        this.plan = plan;
        this.hyperparameters = hyperparameters;
        this.initial_operand = initial_operand;
        this.initial_values = initial_values;
    }


    public void initiateHandshake(){
        FLServerActor.tell(new InitiateHandshakeMessage(clients), ActorRef.noSender());
        try {
            Thread.sleep(5000); // Wait for 5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sendPlanHyperparameters(){
//        FLServerActor.tell(new SendPlanHyperparametersMessage(plan, hyperparameters.get_all_client_hyperparams()), ActorRef.noSender());
//        try {
//            Thread.sleep(5000); // Wait for 5 seconds
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        }
        try {
            Future<Object> future = Patterns.ask(FLServerActor, new SendPlanHyperparametersMessage(plan, hyperparameters.get_all_client_hyperparams()), 500000);
            Await.result(future, Duration.create(500, TimeUnit.SECONDS));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Boolean checkCriterion(){
        try {
            Future<Object> future = Patterns.ask(FLServerActor, new CheckCriterionMessage(), 500000);
            return (Boolean) Await.result(future, Duration.create(500, TimeUnit.SECONDS));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Object runIteration(){
        FLServerActor.tell(new RunIterationMessage(), ActorRef.noSender());
        try {
            Thread.sleep(5000); // Wait for 5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            Future<Object> future = Patterns.ask(FLServerActor, new AggregateResponsesMessage(), 5000);
            return (Object) Await.result(future, Duration.create(5, TimeUnit.SECONDS));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void updateState(Object aggregatedResult){
        FLServerActor.tell(new UpdateStateMessage(aggregatedResult), ActorRef.noSender());
    }

    public Object getFinalOperand(){
        try {
            Future<Object> future = Patterns.ask(FLServerActor, new FinalOperandMessage(), 5000);
            return (Object) Await.result(future, Duration.create(5, TimeUnit.SECONDS));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

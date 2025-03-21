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

package org.test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pekko.actor.ActorSystem;
import org.client.FLClientApp;
import org.components.FLJob;
import org.components.FLSystem;
import org.components.aggregator.Aggregator;
import org.components.criterion.Criterion;
import org.components.hyperparameters.Hyperparameters;
import org.functions.AggregatorFunction;
import org.functions.PlanFunction;
import org.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FLIntegrationTest {
    private static Config server_config;
    private Config client_config;
    @BeforeAll
    public static void setup() {
        // Setup any global resources if needed.
        server_config = ConfigFactory.load("server-application");
//        system = ActorSystem.create("server-system", config);
        System.out.println("Starting FLIntegrationTest...");
    }

    @AfterAll
    public static void tearDown() {
        // Cleanup any global resources if needed.
        System.out.println("FLIntegrationTest complete.");
    }

    @Test
    public void testFLWorkflow() throws Exception {
        // Create a dummy server instance.
        Server server = new Server("pekko://server-system@127.0.0.1:2551", "server");

        // Define client names and URLs (adjust the URLs for your actual remote actors).
        List<String> clientNames = Arrays.asList("client1", "client2", "client3");
        List<String> clientUrls = Arrays.asList(
                "pekko://client1-system@127.0.0.1:2552/user/client1",
                "pekko://client2-system@127.0.0.1:2553/user/client2",
                "pekko://client3-system@127.0.0.1:2554/user/client3"
        );
        List<String> clientConfigs = Arrays.asList("client1-application", "client2-application", "client3-application");


        AggregatorFunction aggregatorFunction = (clientResponses, serverHyperparams) -> {
            // Check if there are no responses
            if (clientResponses == null || clientResponses.isEmpty()) {
                return List.of();
            }
            // Assume each response is a List<Double>
            // Initialize an aggregated list with zeros using the size of the first response
            List<Double> firstList = (List<Double>) clientResponses.get(0);
            int size = firstList.size();
            List<Double> aggregated = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                aggregated.add(0.0);
            }
            // Iterate through each response and add the values element-wise
            for (Object response : clientResponses) {
                List<Double> values = (List<Double>) response;
                for (int i = 0; i < values.size(); i++) {
                    aggregated.set(i, aggregated.get(i) + values.get(i));
                }
            }
            return aggregated;
        };

//        AggregatorFunction aggregatorFunction = (clientResponses, serverHyperparams) -> {
//            // Check if there are no responses
//            if (clientResponses == null || clientResponses.isEmpty()) {
//                return List.of();
//            }
//            // Assume each response is a List<Double>
//            // Initialize an aggregated list with zeros using the size of the first response
//            Integer answer = 0;
//            for(Object response : clientResponses) answer += (Integer) response;
//            Object aggregated = (Object) answer;
//            return aggregated;
//        };

//        for(int i = 0; i < clientNames.size(); i++){
//            FLClientApp client_app = new FLClientApp(clientUrls.get(i), clientNames.get(i), "java");
//            client_config = ConfigFactory.load(clientConfigs.get(i));
//            client_app.startFLClient(client_config);
//        }


        // A simple aggregator that just returns the responses as-is.
        Aggregator aggregator = new Aggregator(aggregatorFunction);

        // A dummy criterion that always returns true.
        Criterion criterion = new Criterion(f -> ((Integer) f.get("epoch")) < 5);

        // Dummy hyperparameters (assumes a default constructor).
        Hyperparameters hyperparameters = new Hyperparameters();

        // A dummy plan function that does nothing.
        PlanFunction planFunction = (w, pb, m) -> pb
                .loadCollection((List<Double>)w).withName("init weights")
                .map(value -> value + 2.0)
                .withName("Square elements")
                .dataQuanta()
                .operator();

//        PlanFunction planFunction = (w, pb, m) -> (Integer)w + 1;

        // Dummy initial values and operand.
        Map<String, Object> initialValues = new HashMap<>();
        initialValues.put("epoch", 0);
        Object initialOperand = (Object) new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));
//        Object initialOperand = (Object) 1;

        // Dummy update rules (empty in this case).
        Map<String, Function<Object, Object>> updateRules = new HashMap<>();
        updateRules.put("epoch", epoch -> (int)epoch + 1);

        // Dummy update operand function that simply returns the first part of the pair.
        Function<Pair<Object, Object>, Object> updateOperand = pair -> pair.getRight();

        FLSystem flSystem = new FLSystem(server.getName(), server.getUrl(), clientNames, clientUrls);


        // Create an FLJob instance with the dummy dependencies.
        FLJob job = new FLJob("TestJob", clientNames, clientUrls,
                planFunction, hyperparameters, criterion, aggregator,
                initialValues, initialOperand, updateRules, updateOperand);

        // Create the FLSystem using the server information and client lists.

        // Register the job with the system.
        String jobId = flSystem.registerFLJob(job, server_config);
        System.out.println("Registered job with ID: " + jobId);

        // Start the job. This method calls sendPlanHyperparameters(), iterates until the criterion is false,
        // and eventually updates the state.
        flSystem.startFLJob(jobId);

        // Obtain the final result from the job.
        Object finalResult = flSystem.getFLJobResult(jobId);
        System.out.println("Final result: " + finalResult);

        // Write the final result to a file in the target/test-output folder.
        Path outputPath = Path.of("target", "test-output.txt");
        Files.createDirectories(outputPath.getParent());
        Files.write(outputPath, finalResult.toString().getBytes());
        System.out.println("Output written to " + outputPath.toAbsolutePath());

        // Assert that the result is not null.
        assertNotNull(finalResult, "The final result should not be null");
    }
}

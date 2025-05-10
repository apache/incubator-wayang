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

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;

public class FLClientApp {
    private final Client client;
    private final String platform_type;
    private final String[] inputFiles;

    public FLClientApp(String client_url, String client_id, String platform_type, String[] inputFiles){
        this.client = new Client(client_url, client_id);
        this.platform_type = platform_type;
        this.inputFiles = inputFiles;
    }

    public void startFLClient(Config config){
        ActorSystem system = ActorSystem.create(client.getName() + "-system", config);
        ActorRef FLClientActor = system.actorOf(
                Props.create(FLClient.class, () -> new FLClient(
                        client, platform_type, inputFiles
                )),
                client.getName()
        );
        System.out.println(client.getName() + " is running");
    }
}

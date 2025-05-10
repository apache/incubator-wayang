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
import org.client.FLClientApp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class FLClientTest2 {
    @BeforeAll
    public static void setup() {
        System.out.println("Starting FLClient test...");
    }

    public static void main(String args[]) throws Exception {
        FLClientApp client_app = new FLClientApp("pekko://client2-system@127.0.0.1:2553/user/client2", "client2", "java", new String[] {"file:/Users/vedantaneogi/Downloads/higgs_part2.txt"});
        Config config = ConfigFactory.load("client2-application");
        client_app.startFLClient(config);
    }
}

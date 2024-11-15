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

package org.apache.wayang.basic.operators;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class GoogleCloudStorageTest {

    private final Logger logger = LogManager.getLogger(this.getClass());

    @Test
    public void estimateBytesPerLine() throws IOException {

        String filePath = "/Users/christofferkristensen/Documents/SoftwareDesign/research_project/ResearchProject/long-centaur-438410-p7-90933b1671ea.json";

    //     String bucketName = "wayang-test-bucket";
    //     String blobName = "GCS-sample.txt";

    //     var source = new GoogleCloudStorageSource(bucketName, filePath, "");

    //     var doubles = source.GetEstimateBytesPerLine(blobName);

    //     System.out.println("Found estimated bytes per line " + doubles);

    //     assertTrue(true);
    // }
    }
}

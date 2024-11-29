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


import org.apache.wayang.core.util.LimitedInputStream;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Stream;

import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;

import java.io.InputStream;

public class AzureStorageTest {

    @Test
    public void ReadBlob(){


    }

    private static void Print(String str) {
        System.out.println(str);
    }

    //total blob size
    private static Long GetBlobSize(BlobClient blobClient) {
        return blobClient.getProperties().getBlobSize();
    }

    private double EstimateBytesPerLine(BlobClient blobClient) throws IOException {

        // 1 MiB limit
        final int KiB = 1024;
        final int MiB = KiB * 1024;
        final long readLimit = 1L * MiB;
        String encoding = "UTF-8"; // Replace with your desired encoding if needed


        InputStream inputStream = blobClient.openInputStream();

        LimitedInputStream limitedInputStream = new LimitedInputStream(inputStream, readLimit);

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(limitedInputStream, encoding));

            // Read as much as possible
            char[] cbuf = new char[1024];
            int numReadChars, numLineFeeds = 0;

            while ((numReadChars = bufferedReader.read(cbuf)) != -1) {
                for (int i = 0; i < numReadChars; i++) {
                    if (cbuf[i] == '\n') {
                        numLineFeeds++;
                    }
                }
            }

        return 0.0;
    }
}
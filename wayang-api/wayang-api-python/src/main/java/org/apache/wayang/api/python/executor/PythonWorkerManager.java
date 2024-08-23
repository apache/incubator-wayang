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

package org.apache.wayang.api.python.executor;

import com.google.protobuf.ByteString;
import org.apache.wayang.core.api.exception.WayangException;

public class PythonWorkerManager<Input, Output> {

    private ByteString serializedUDF;
    private Iterable<Input> inputIterator;

    public PythonWorkerManager(
            ByteString serializedUDF,
            Iterable<Input> input
    ){
        this.serializedUDF = serializedUDF;
        this.inputIterator = input;
    }

    public Iterable<Output> execute(){
        PythonProcessCaller worker = new PythonProcessCaller(this.serializedUDF);

        if(worker.isReady()){
            ProcessFeeder<Input, Output> feed = new ProcessFeeder<>(
                worker.getSocket(),
                this.serializedUDF,
                this.inputIterator
            );
            feed.send();
            ProcessReceiver<Output> r = new ProcessReceiver<>(worker.getSocket());
            return r.getIterable();
        } else{
            int port = worker.getSocket().getLocalPort();
            worker.close();
            throw new WayangException("Not possible to work with the Socket provided on port: " + port);
        }

    }
}

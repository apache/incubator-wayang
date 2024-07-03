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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;

/*TODO cannot be always string, include definition for every operator
*  like: map(udf, inputtype, outputtype)*/
public class ProcessReceiver<Output> {

    private ReaderIterator<Output> iterator;

    public ProcessReceiver(Socket socket){
        try{
            //TODO use config buffer size
            int BUFFER_SIZE = 8192;

            DataInputStream stream = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
            this.iterator = new ReaderIterator<>(stream);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Iterable<Output> getIterable(){
        return () -> iterator;
    }

    public void print(){
        iterator.forEachRemaining(x -> System.out.println(x.toString()));
    }
}

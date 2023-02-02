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

package org.apache.wayang.agoraeo.iterators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public abstract class IteratorSentinelDownload<Input> implements Iterator<Input>, Serializable {
    private String[] command;
    private Process process = null;
    private Iterator<Input> iteratorProcess = null;

    public IteratorSentinelDownload(String python_location, String module_name, Map<String, String> params) {
        this(python_location, module_name, "--%s", params);
    }

    public IteratorSentinelDownload(String python_location, String module_name, String format, Map<String, String> params) {

        System.out.println("python_location: " + python_location);
        System.out.println("module_name: " + module_name);
        System.out.println("format: " + format);
        System.out.println("params: " + params);
        this.command = new String[(params.size()*2) + 2];
        this.command[0] = python_location;
        this.command[1] = module_name;
        int position = 2;
//        System.out.println(
//                String.format(
//                        "first command: %s",
//                        Arrays.toString(this.command)
//                )
//        );
        for (Map.Entry<String, String> param : params.entrySet()) {
            this.command[position] = String.format(format, param.getKey());
            this.command[position + 1] = param.getValue();
            position += 2;
        }
        System.out.println(
            String.format(
                "command: %s",
                Arrays.toString(this.command)
            )
        );
    }

    @Override
    public boolean hasNext() {
        if(this.process == null){
            return startProcess();
        }
        if( !this.process.isAlive() ){
            return this.iteratorProcess.hasNext();
        }
        return true;
    }

    @Override
    public Input next() {
        if( ! this.iteratorProcess.hasNext()){
            return this.getDefaultValue();
        }
        return this.iteratorProcess.next();
    }

    private boolean startProcess(){
        try {
            this.process = Runtime.getRuntime().exec(this.command);
            this.iteratorProcess = getLogic(
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream()
                            )
                    ).lines()
            ).iterator();
            return true;
        } catch (IOException e) {}
        return false;
    }

    protected abstract Stream<Input> getLogic(Stream<String> baseline);

    protected abstract Input getDefaultValue();

}

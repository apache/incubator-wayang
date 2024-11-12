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

package org.apache.wayang.flink.compiler;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.wayang.core.function.ConsumerDescriptor;

import java.io.IOException;
import java.io.Serializable;

/**
 * Wrapper for {@Link OutputFormat}
 */
public class OutputFormatConsumer<T> implements OutputFormat<T>, Serializable {

    private ConsumerDescriptor.SerializableConsumer<T> tConsumer;

    public OutputFormatConsumer(ConsumerDescriptor.SerializableConsumer<T> consumer) {
        this.tConsumer = consumer;
    }


    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
    }

    @Override
    public void writeRecord(T o) throws IOException {
        this.tConsumer.accept(o);
    }

    @Override
    public void close() throws IOException {

    }
}

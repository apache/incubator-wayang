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

package org.apache.wayang.flink.test;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.java.channels.CollectionChannel;
import org.junit.Before;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.Collection;

import static org.mockito.Mockito.mock;

/**
 * Utility to create {@link Channel}s in tests.
 */
public class ChannelFactory {

    private static FlinkExecutor flinkExecutor;

    @Before
    public void setUp() {
        flinkExecutor = mock(FlinkExecutor.class);
    }

    public static DataSetChannel.Instance createDataSetChannelInstance(ChannelDescriptor dataSetChannelDescriptor, Configuration configuration){
        return (DataSetChannel.Instance) dataSetChannelDescriptor
                .createChannel(null,configuration)
                .createInstance(flinkExecutor,null,-1);
    }

    public static DataSetChannel.Instance createDataSetChannelInstance(Configuration configuration){
        return createDataSetChannelInstance(DataSetChannel.DESCRIPTOR, configuration);
    }

    public static DataSetChannel.Instance createDataSetChannelInstance(Collection<?> data,
                                                                       FlinkExecutor flinkExecutor,
                                                                       Configuration configuration){
        DataSetChannel.Instance instance = createDataSetChannelInstance(configuration);
        instance.accept(flinkExecutor.fee.fromCollection(WayangCollections.asList(data)),flinkExecutor);
        return instance;
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Configuration configuration) {
        return (CollectionChannel.Instance) CollectionChannel.DESCRIPTOR
                .createChannel(null, configuration)
                .createInstance(flinkExecutor, null, -1);
    }

    public static CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection, Configuration configuration) {
        CollectionChannel.Instance instance = createCollectionChannelInstance(configuration);
        instance.accept(collection);
        return instance;
    }

}

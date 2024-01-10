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

package org.apache.wayang.flink.channels;

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.flink.operators.FlinkCollectionSink;
import org.apache.wayang.flink.operators.FlinkCollectionSource;
import org.apache.wayang.flink.operators.FlinkObjectFileSink;
import org.apache.wayang.flink.operators.FlinkObjectFileSource;
import org.apache.wayang.flink.operators.FlinkTsvFileSink;
import org.apache.wayang.flink.platform.FlinkPlatform;
import org.apache.wayang.java.channels.CollectionChannel;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s used by the {@link FlinkPlatform}.
 */
public class ChannelConversions {

        public static final ChannelConversion COLLECTION_TO_DATASET = new DefaultChannelConversion(
                CollectionChannel.DESCRIPTOR,
                DataSetChannel.DESCRIPTOR,
                () -> new FlinkCollectionSource<>(DataSetType.createDefault(Void.class))
        );

        public static final ChannelConversion DATASET_TO_COLLECTION = new DefaultChannelConversion(
                DataSetChannel.DESCRIPTOR,
                CollectionChannel.DESCRIPTOR,
                () -> new FlinkCollectionSink<>(DataSetType.createDefault(Void.class))
        );

        public static final ChannelConversion OBJECT_FILE_TO_DATASET = new DefaultChannelConversion(
                FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
                DataSetChannel.DESCRIPTOR,
                () -> new FlinkObjectFileSource<>(DataSetType.createDefault(Void.class))
        );

        public static final ChannelConversion DATASET_TO_OBJECT_FILE = new DefaultChannelConversion(
                DataSetChannel.DESCRIPTOR,
                FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR,
                () -> new FlinkObjectFileSink<>(DataSetType.createDefault(Void.class))
        );
        public static final ChannelConversion DATASET_TO_HDFS_TSV = new DefaultChannelConversion(
                DataSetChannel.DESCRIPTOR,
                FileChannel.HDFS_TSV_DESCRIPTOR,
                () -> new FlinkTsvFileSink<>(DataSetType.createDefaultUnchecked(Tuple2.class))
        );

        public static Collection<ChannelConversion> ALL = Arrays.asList(
            COLLECTION_TO_DATASET,
            DATASET_TO_COLLECTION,
            OBJECT_FILE_TO_DATASET,
            DATASET_TO_OBJECT_FILE,
            DATASET_TO_HDFS_TSV
        );
}

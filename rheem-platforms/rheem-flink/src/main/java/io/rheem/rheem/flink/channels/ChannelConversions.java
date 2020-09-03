package io.rheem.rheem.flink.channels;

import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.basic.data.Tuple2;
import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.optimizer.channels.DefaultChannelConversion;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.flink.operators.FlinkCollectionSink;
import io.rheem.rheem.flink.operators.FlinkCollectionSource;
import io.rheem.rheem.flink.operators.FlinkObjectFileSink;
import io.rheem.rheem.flink.operators.FlinkObjectFileSource;
import io.rheem.rheem.flink.operators.FlinkTsvFileSink;
import io.rheem.rheem.flink.platform.FlinkPlatform;
import io.rheem.rheem.java.channels.CollectionChannel;

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

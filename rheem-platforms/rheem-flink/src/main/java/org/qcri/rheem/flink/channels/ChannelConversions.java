package org.qcri.rheem.flink.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.flink.operators.FlinkCollectionSink;
import org.qcri.rheem.flink.operators.FlinkCollectionSource;
import org.qcri.rheem.flink.operators.FlinkObjectFileSink;
import org.qcri.rheem.flink.operators.FlinkObjectFileSource;
import org.qcri.rheem.flink.operators.FlinkTsvFileSink;
import org.qcri.rheem.flink.platform.FlinkPlatform;
import org.qcri.rheem.java.channels.CollectionChannel;

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

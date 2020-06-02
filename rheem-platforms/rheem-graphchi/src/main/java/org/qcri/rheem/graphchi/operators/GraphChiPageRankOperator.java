package org.qcri.rheem.graphchi.operators;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.apps.Pagerank;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.ConsumerIteratorAdapter;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.core.util.fs.LocalFileSystem;
import org.qcri.rheem.graphchi.platform.GraphChiPlatform;
import org.qcri.rheem.java.channels.StreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * PageRank {@link Operator} implementation for the {@link GraphChiPlatform}.
 */
public class GraphChiPageRankOperator extends PageRankOperator implements GraphChiExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public GraphChiPageRankOperator(Integer numIterations) {
        super(numIterations);
    }

    public GraphChiPageRankOperator(PageRankOperator pageRankOperator) {
        super(pageRankOperator);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> execute(
            ChannelInstance[] inputChannelInstances,
            ChannelInstance[] outputChannelInstances,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputChannelInstances.length == this.getNumInputs();
        assert outputChannelInstances.length == this.getNumOutputs();

        final FileChannel.Instance inputChannelInstance = (FileChannel.Instance) inputChannelInstances[0];
        final StreamChannel.Instance outputChannelInstance = (StreamChannel.Instance) outputChannelInstances[0];
        try {
            return this.runGraphChi(inputChannelInstance, outputChannelInstance, operatorContext);
        } catch (IOException e) {
            throw new RheemException(String.format("Running %s failed.", this), e);
        }
    }

    private Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> runGraphChi(
            FileChannel.Instance inputFileChannelInstance,
            StreamChannel.Instance outputChannelInstance,
            OptimizationContext.OperatorContext operatorContext)
            throws IOException {

        assert inputFileChannelInstance.wasProduced();

        final String inputPath = inputFileChannelInstance.getSinglePath();
        final String actualInputPath = FileSystems.findActualSingleInputPath(inputPath);
        final FileSystem inputFs = FileSystems.getFileSystem(inputPath).orElseThrow(
                () -> new RheemException(String.format("Could not identify filesystem for \"%s\".", inputPath))
        );

        // Create shards.
        Configuration configuration = operatorContext.getOptimizationContext().getConfiguration();
        String tempDirPath = configuration.getStringProperty("rheem.graphchi.tempdir");
        Random random = new Random();
        String tempFilePath = String.format("%s%s%04x-%04x-%04x-%04x.tmp", tempDirPath, File.separator,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF
        );
        final File tempFile = new File(tempFilePath);
        LocalFileSystem.touch(tempFile);
        tempFile.deleteOnExit();
        String graphName = tempFile.toString();
        // As suggested by GraphChi, we propose to use approximately 1 shard per 1,000,000 edges.
        final int numShards = 2 + (int) inputFs.getFileSize(actualInputPath) / (10 * 1000000);
        if (!new File(ChiFilenames.getFilenameIntervals(graphName, numShards)).exists()) {
            FastSharder sharder = createSharder(graphName, numShards);
            final InputStream inputStream = inputFs.open(actualInputPath);
            sharder.shard(inputStream, FastSharder.GraphInputFormat.EDGELIST);
        } else {
            this.logger.info("Found shards -- no need to preprocess");
        }

        // Run GraphChi.
        GraphChiEngine<Float, Float> engine = new GraphChiEngine<>(graphName, numShards);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatConverter());
        engine.setModifiesInedges(false); // Important optimization
        engine.run(new Pagerank(), this.numIterations);

        final ConsumerIteratorAdapter<Tuple2<Long, Float>> consumerIteratorAdapter = new ConsumerIteratorAdapter<>();
        final Consumer<Tuple2<Long, Float>> consumer = consumerIteratorAdapter.getConsumer();
        final Iterator<Tuple2<Long, Float>> iterator = consumerIteratorAdapter.getIterator();

        // Output results.
        VertexIdTranslate trans = engine.getVertexIdTranslate();
        new Thread(
                () -> {
                    try {
                        VertexAggregator.foreach(engine.numVertices(), graphName, new FloatConverter(),
                                (vertexId, vertexValue) -> consumer.accept(new Tuple2<>((long) trans.backward(vertexId), vertexValue)));
                    } catch (IOException e) {
                        throw new RheemException(e);
                    } finally {
                        consumerIteratorAdapter.declareLastAdd();
                    }
                },
                String.format("%s (output)", this)
        ).start();

        Stream<Tuple2<Long, Float>> outputStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        outputChannelInstance.accept(outputStream);

        // Model what has been executed.
        final ExecutionLineageNode mainExecutionLineage = new ExecutionLineageNode(operatorContext);
        mainExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "rheem.graphchi.pagerank.load.main", configuration
        ));
        mainExecutionLineage.addPredecessor(inputFileChannelInstance.getLineage());

        final ExecutionLineageNode outputExecutionLineage = new ExecutionLineageNode(operatorContext);
        outputExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "rheem.graphchi.pagerank.load.output", configuration
        ));
        outputChannelInstance.getLineage().addPredecessor(outputExecutionLineage);

        return mainExecutionLineage.collectAndMark();
    }

    /**
     * Initialize the sharder-program.
     *
     * @param graphName
     * @param numShards
     * @return
     * @throws IOException
     */
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<>(
                graphName,
                numShards,
                (vertexId, token) ->
                        (token == null ? 0.0f : Float.parseFloat(token)),
                (from, to, token) ->
                        (token == null ? 0.0f : Float.parseFloat(token)),
                new FloatConverter(),
                new FloatConverter());
    }


    @Override
    public Platform getPlatform() {
        return GraphChiPlatform.getInstance();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.graphchi.pagerank.load.main", "rheem.graphchi.pagerank.load.output");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}

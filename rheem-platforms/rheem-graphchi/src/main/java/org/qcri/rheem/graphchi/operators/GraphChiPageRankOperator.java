package org.qcri.rheem.graphchi.operators;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.apps.Pagerank;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.graphchi.GraphChiPlatform;
import org.qcri.rheem.graphchi.channels.ChannelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * PageRank {@link Operator} implementation for the {@link GraphChiPlatform}.
 */
public class GraphChiPageRankOperator extends PageRankOperator implements GraphChiOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public GraphChiPageRankOperator(int numIterations) {
        super(numIterations);
    }

    public GraphChiPageRankOperator(PageRankOperator pageRankOperator) {
        super(pageRankOperator.getNumIterations());
    }

    @Override
    public void execute(Channel[] inputChannels, Channel[] outputChannels) {
        assert inputChannels.length == this.getNumInputs();
        assert outputChannels.length == this.getNumOutputs();

        final FileChannel inputFileChannel = (FileChannel) inputChannels[0];
        final FileChannel outputFileChannel = (FileChannel) outputChannels[0];
        try {
            this.runGraphChi(inputFileChannel, outputFileChannel);
        } catch (IOException e) {
            throw new RheemException(String.format("Running %s failed.", this), e);
        }
    }

    private void runGraphChi(FileChannel inputFileChannel, FileChannel outputFileChannel) throws IOException {

        final String inputPath = inputFileChannel.getSinglePath();
        final FileSystem inputFs = FileSystems.getFileSystem(inputPath).get();

        // Create shards.
        String graphName = File.createTempFile("rheem-graphchi", "graph").toString();
//        String graphName = String.format("rheem-graphchi-%06x", new Random().nextInt(0xFFFFFF));
        // As suggested by GraphChi, we propose to use approximately 1 shard per 1,000,000 edges.
        final int numShards = 2 + (int) inputFs.getFileSize(inputPath) / (10 * 1000000);
        if (!new File(ChiFilenames.getFilenameIntervals(graphName, numShards)).exists()) {
            FastSharder sharder = createSharder(graphName, numShards);
            final InputStream inputStream = inputFs.open(inputPath);
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

        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannel.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(outFs.create(outputFileChannel.getSinglePath()))) {
            VertexIdTranslate trans = engine.getVertexIdTranslate();
            VertexAggregator.foreach(engine.numVertices(), graphName, new FloatConverter(),
                    (vertexId, vertexValue) -> {
                        try {
                            writer.write(String.valueOf(trans.backward(vertexId)));
                            writer.write('\t');
                            writer.write(String.valueOf(vertexValue));
                            writer.write('\n');
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }

                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
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
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(ChannelManager.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(ChannelManager.HDFS_TSV_DESCRIPTOR);
    }

}

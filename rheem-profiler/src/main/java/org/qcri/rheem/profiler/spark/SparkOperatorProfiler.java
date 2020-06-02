package org.qcri.rheem.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.profiler.util.RrdAccessor;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import org.rrd4j.ConsolFun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Allows to instrument an {@link SparkExecutionOperator}.
 */
public abstract class SparkOperatorProfiler {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Supplier<SparkExecutionOperator> operatorGenerator;

    protected final List<Supplier<?>> dataQuantumGenerators;

    private final String gangliaRrdsDir;

    private final String gangliaClusterName;

    public int cpuMhz, numMachines, numCoresPerMachine, numPartitions;

    private final int dataQuantumGeneratorBatchSize;

    private final String dataQuantumGeneratorLocation;

    protected final long executionPaddingTime;

    protected SparkExecutionOperator operator;

    protected SparkExecutor sparkExecutor;

    protected final FunctionCompiler functionCompiler = new FunctionCompiler();

    protected List<Long> inputCardinalities;

    public SparkOperatorProfiler(Supplier<SparkExecutionOperator> operatorGenerator,
                                 Configuration configuration,
                                 Supplier<?>... dataQuantumGenerators) {

        this.operatorGenerator = operatorGenerator;
        this.dataQuantumGenerators = Arrays.asList(dataQuantumGenerators);

        this.cpuMhz = (int) configuration.getLongProperty("rheem.spark.cpu.mhz", 2700);
        this.numMachines = (int) configuration.getLongProperty("rheem.spark.machines", 1);
        this.numCoresPerMachine = (int) configuration.getLongProperty("rheem.spark.cores-per-machine", 1);
        this.numPartitions = (int) configuration.getLongProperty("rheem.spark.partitions", -1);

        this.gangliaRrdsDir = configuration.getStringProperty("rheem.ganglia.rrds", "/var/lib/ganglia/rrds");
        this.gangliaClusterName = configuration.getStringProperty("rheem.ganglia.cluster", "cluster");

        this.dataQuantumGeneratorBatchSize = (int) configuration.getLongProperty("rheem.profiler.datagen.batchsize", 5000000);
        this.dataQuantumGeneratorLocation = configuration.getStringProperty("rheem.profiler.datagen.location", "worker");
        this.executionPaddingTime = configuration.getLongProperty("rheem.profiler.execute.padding", 5000);
    }

    /**
     * Call this method before {@link #run()} to prepare the profiling run
     *
     * @param inputCardinalities number of input elements for each input of the profiled operator
     */
    public void prepare(long... inputCardinalities) {
        this.operator = this.operatorGenerator.get();
        this.inputCardinalities = RheemArrays.asList(inputCardinalities);
        this.sparkExecutor = ProfilingUtils.fakeSparkExecutor(ReflectionUtils.getDeclaringJar(SparkOperatorProfiler.class));
        for (int inputIndex = 0; inputIndex < inputCardinalities.length; inputIndex++) {
            long inputCardinality = inputCardinalities[inputIndex];
            this.prepareInput(inputIndex, inputCardinality);
        }
    }

    protected abstract void prepareInput(int inputIndex, long inputCardinality);

    /**
     * Helper method to generate data quanta and provide them as a cached {@link JavaRDD}. Uses an implementation
     * based on the {@code rheem.profiler.datagen.location} property.
     */
    protected <T> JavaRDD<T> prepareInputRdd(long cardinality, int inputIndex) {
        switch (this.dataQuantumGeneratorLocation) {
            case "worker":
                return this.prepareInputRddInWorker(cardinality, inputIndex);
            case "driver":
                return this.prepareInputRddInDriver(cardinality, inputIndex);
            default:
                this.logger.error("In correct data generation location (is: {}, allowed: worker/driver). Using worker.");
                return this.prepareInputRddInWorker(cardinality, inputIndex);
        }
    }

    /**
     * Helper method to generate data quanta and provide them as a cached {@link JavaRDD}.
     */
    protected <T> JavaRDD<T> prepareInputRddInDriver(long cardinality, int inputIndex) {
        @SuppressWarnings("unchecked")
        final Supplier<T> supplier = (Supplier<T>) this.dataQuantumGenerators.get(inputIndex);
        JavaRDD<T> finalInputRdd = null;

        // Create batches, parallelize them, and union them.
        long remainder = cardinality;
        do {
            int batchSize = (int) Math.min(remainder, this.dataQuantumGeneratorBatchSize);
            List<T> batch = new ArrayList<>(batchSize);
            while (batch.size() < batchSize) {
                batch.add(supplier.get());
            }
            final JavaRDD<T> batchRdd = this.sparkExecutor.sc.parallelize(batch);
            finalInputRdd = finalInputRdd == null ? batchRdd : finalInputRdd.union(batchRdd);
            remainder -= batchSize;
        } while (remainder > 0);

        // Shuffle and cache the RDD.
        final JavaRDD<T> cachedInputRdd = this.partition(finalInputRdd).cache();
        cachedInputRdd.foreach(dataQuantum -> {
        });

        return cachedInputRdd;
    }

    /**
     * Helper method to generate data quanta and provide them as a cached {@link JavaRDD}.
     */
    protected <T> JavaRDD<T> prepareInputRddInWorker(long cardinality, int inputIndex) {

        // Create batches, parallelize them, and union them.
        final List<Integer> batchSizes = new LinkedList<>();
        int numFullBatches = (int) (cardinality / this.dataQuantumGeneratorBatchSize);
        for (int i = 0; i < numFullBatches; i++) {
            batchSizes.add(this.dataQuantumGeneratorBatchSize);
        }
        batchSizes.add((int) (cardinality % this.dataQuantumGeneratorBatchSize));

        @SuppressWarnings("unchecked")
        final Supplier<T> supplier = (Supplier<T>) this.dataQuantumGenerators.get(inputIndex);
        JavaRDD<T> finalInputRdd = this.sparkExecutor.sc
                .parallelize(batchSizes, 1) // Single partition to ensure the same data generator.
                .flatMap(batchSize -> {
                    List<T> list = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        list.add(supplier.get());
                    }
                    return list.iterator();
                });
        // Shuffle and cache the RDD.
        final JavaRDD<T> cachedInputRdd = this.partition(finalInputRdd).cache();
        cachedInputRdd.foreach(dataQuantum -> {
        });

        return cachedInputRdd;
    }

    /**
     * If a desired number of partitions for the input {@link JavaRDD}s is requested, enforce this.
     */
    protected <T> JavaRDD<T> partition(JavaRDD<T> rdd) {
        return this.numPartitions == -1 ? rdd : rdd.coalesce(this.numPartitions, true);
    }

    /**
     * Executes and profiles the profiling task. Requires that this instance is prepared.
     */
    public Result run() {
        final Result result = this.executeOperator();
        this.sparkExecutor.dispose();
        this.sparkExecutor = null;
        return result;
    }


    /**
     * Estimates the disk bytes occurred in the cluster during the given time span by waiting for Ganglia to provide
     * the respective information in its RRD files.
     */
    protected long provideCpuCycles(long startTime, long endTime) {
        // Find out the average idle fraction in the cluster.
        final double sumCpuIdleRatio = this.waitAndQueryMetricAverage("cpu_idle", "sum", startTime, endTime);
        final double numCpuIdleRatio = this.waitAndQueryMetricAverage("cpu_idle", "num", startTime, endTime);
        final double avgCpuIdleRatio = sumCpuIdleRatio / numCpuIdleRatio / 100;

        // Determine number of cycles per millisecond.
        long passedMillis = endTime - startTime;
        double cyclesPerMillis = this.cpuMhz * 1e3 * this.numCoresPerMachine * this.numMachines;

        // Estimate the number of spent CPU cycles in the cluster.
        return Math.round(passedMillis * cyclesPerMillis * (1 - avgCpuIdleRatio));
    }

    /**
     * Estimates the network bytes occurred in the cluster during the given time span by waiting for Ganglia to provide
     * the respective information in its RRD files.
     */
    protected long provideNetworkBytes(long startTime, long endTime) {
        // Find out the average received/transmitted bytes per second.
        final double transmittedBytesPerSec = this.waitAndQueryMetricAverage("tx_bytes_eth0", "sum", startTime, endTime);
        final double receivedBytesPerSec = this.waitAndQueryMetricAverage("rx_bytes_eth0", "sum", startTime, endTime);
        final double bytesPerSec = (transmittedBytesPerSec + receivedBytesPerSec) / 2;

        // Estimate the number of actually communicated bytes.
        return (long) (bytesPerSec / 1000 * (endTime - startTime));

    }

    /**
     * Estimates the disk bytes occurred in the cluster during the given time span by waiting for Ganglia to provide
     * the respective information in its RRD files.
     */
    protected long provideDiskBytes(long startTime, long endTime) {
        // Find out the average received/transmitted bytes per second.
        final double readBytesPerSec = this.waitAndQueryMetricAverage("diskstat_sda_read_bytes_per_sec", "sum", startTime, endTime);
        final double writeBytesPerSec = this.waitAndQueryMetricAverage("diskstat_sda_read_bytes_per_sec", "sum", startTime, endTime);
        final double bytesPerSec = readBytesPerSec + writeBytesPerSec;

        // Estimate the number of actually communicated bytes.
        return (long) (bytesPerSec / 1000 * (endTime - startTime));
    }

    /**
     * Queries an average metric from a Ganglia RRD file. If the metric is not recent enough, this method waits
     * until the requested data points are available.
     */
    private double waitAndQueryMetricAverage(String metric, String dataSeries, long startTime, long endTime) {
        final String rrdFile = this.gangliaRrdsDir + File.separator +
                this.gangliaClusterName + File.separator +
                "__SummaryInfo__" + File.separator +
                metric + ".rrd";

        double metricValue = Double.NaN;
        int numAttempts = 0;
        do {
            if (numAttempts++ > 0) {
                ProfilingUtils.sleep(5000);
            }

            try (RrdAccessor rrdAccessor = RrdAccessor.open(rrdFile)) {
                final long lastUpdateMillis = rrdAccessor.getLastUpdateMillis();
                if (lastUpdateMillis >= endTime) {
                    metricValue = rrdAccessor.query(dataSeries, startTime, endTime, ConsolFun.AVERAGE);
                } else {
                    this.logger.info("Last RRD file update is only from {} ({} attempts so far).", new Date(lastUpdateMillis), numAttempts);
                }
            } catch (Exception e) {
                this.logger.error(String.format("Could not access RRD %s.", rrdFile), e);
                return Double.NaN;
            }
        } while (Double.isNaN(metricValue));

        return metricValue;
    }

    /**
     * Executes the profiling task. Requires that this instance is prepared.
     */
    protected abstract Result executeOperator();

    /**
     * Utility method to invoke
     * {@link SparkExecutionOperator#evaluate(ChannelInstance[], ChannelInstance[], SparkExecutor, OptimizationContext.OperatorContext)}.
     */
    protected void evaluate(SparkExecutionOperator operator,
                           ChannelInstance[] inputs,
                           ChannelInstance[] outputs) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.sparkExecutor.getJob());
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(operator);
        operator.evaluate(inputs, outputs, this.sparkExecutor, operatorContext);
    }

    /**
     * Creates a {@link ChannelInstance} that carries the given {@code rdd}.
     */
    protected static RddChannel.Instance createChannelInstance(final JavaRDD<?> rdd, SparkExecutor sparkExecutor) {
        final RddChannel.Instance channelInstance = createChannelInstance(sparkExecutor);
        channelInstance.accept(rdd, sparkExecutor);
        return channelInstance;
    }


    /**
     * Creates an empty {@link ChannelInstance}.
     */
    protected static RddChannel.Instance createChannelInstance(SparkExecutor sparkExecutor) {
        final ChannelDescriptor channelDescriptor = RddChannel.CACHED_DESCRIPTOR;
        final RddChannel channel = (RddChannel) channelDescriptor.createChannel(null, sparkExecutor.getConfiguration());
        return (RddChannel.Instance) channel.createInstance(null, null, -1);
    }

    /**
     * Override this method to implement any clean-up logic.
     */
    public void cleanUp() {
    }

    /**
     * The result of a single profiling run.
     */
    public static class Result {

        private final List<Long> inputCardinalities;

        private final int numMachines, numCoresPerMachine;

        private final long outputCardinality;

        private final long diskBytes, networkBytes;

        private final long cpuCycles;

        private final long wallclockMillis;

        public Result(List<Long> inputCardinalities, long outputCardinality,
                      long wallclockMillis, long diskBytes, long networkBytes, long cpuCycles,
                      int numMachines, int numCoresPerMachine) {
            this.inputCardinalities = inputCardinalities;
            this.outputCardinality = outputCardinality;
            this.wallclockMillis = wallclockMillis;
            this.diskBytes = diskBytes;
            this.networkBytes = networkBytes;
            this.cpuCycles = cpuCycles;
            this.numMachines = numMachines;
            this.numCoresPerMachine = numCoresPerMachine;
        }

        public List<Long> getInputCardinalities() {
            return this.inputCardinalities;
        }

        public long getOutputCardinality() {
            return this.outputCardinality;
        }

        public long getDiskBytes() {
            return this.diskBytes;
        }

        public long getNetworkBytes() {
            return this.networkBytes;
        }

        public long getCpuCycles() {
            return this.cpuCycles;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "inputCardinalities=" + inputCardinalities +
                    ", outputCardinality=" + outputCardinality +
                    ", numMachines=" + numMachines +
                    ", numCoresPerMachine=" + numCoresPerMachine +
                    ", wallclockMillis=" + wallclockMillis +
                    ", cpuCycles=" + cpuCycles +
                    ", diskBytes=" + diskBytes +
                    ", networkBytes=" + networkBytes +
                    '}';
        }

        public String getCsvHeader() {
            return String.join(",", RheemCollections.map(this.inputCardinalities, (index, card) -> "input_card_" + index)) + "," +
                    "output_card," +
                    "wallclock," +
                    "disk," +
                    "network," +
                    "cpu," +
                    "machines," +
                    "cores_per_machine";
        }

        public String toCsvString() {
            return String.join(",", RheemCollections.map(this.inputCardinalities, Object::toString)) + ","
                    + this.outputCardinality + ","
                    + this.wallclockMillis + ","
                    + this.diskBytes + ","
                    + this.networkBytes + ","
                    + this.cpuCycles + ","
                    + this.numMachines + ","
                    + this.numCoresPerMachine;
        }
    }

}

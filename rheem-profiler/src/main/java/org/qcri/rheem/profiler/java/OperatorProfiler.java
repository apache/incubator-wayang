package org.qcri.rheem.profiler.java;

import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Allows to instrument an {@link JavaExecutionOperator}.
 */
public abstract class OperatorProfiler {

    public int cpuMhz;

    protected Supplier<JavaExecutionOperator> operatorGenerator;

    protected JavaExecutionOperator operator;

    protected final List<Supplier<?>> dataQuantumGenerators;

    private List<Long> inputCardinalities;

    public OperatorProfiler(Supplier<JavaExecutionOperator> operatorGenerator,
                            Supplier<?>... dataQuantumGenerators) {
        this.operatorGenerator = operatorGenerator;
        this.dataQuantumGenerators = Arrays.asList(dataQuantumGenerators);
        this.cpuMhz = Integer.parseInt(System.getProperty("rheem.java.cpu.mhz", "2700"));
    }


    public void prepare(long... inputCardinalities) {
        this.operator = this.operatorGenerator.get();
        this.inputCardinalities = RheemArrays.asList(inputCardinalities);
    }


    /**
     * Executes and profiles the profiling task. Requires that this instance is prepared.
     */
    public Result run() {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        threadMXBean.setThreadCpuTimeEnabled(true);
        sleep(1000);
        long startCpuTime = threadMXBean.getCurrentThreadCpuTime();
        final long outputCardinality = this.executeOperator();
        long endCpuTime = threadMXBean.getCurrentThreadCpuTime();

        long cpuCycles = this.calculateCpuCycles(startCpuTime, endCpuTime);
        return new Result(
                this.inputCardinalities,
                outputCardinality,
                this.provideDiskBytes(),
                this.provideNetworkBytes(),
                cpuCycles
        );
    }

    private long calculateCpuCycles(long startNanos, long endNanos) {
        long passedNanos = endNanos - startNanos;
        double cyclesPerNano = (this.cpuMhz * 1e6) / 1e9;
        return Math.round(cyclesPerNano * passedNanos);
    }

    protected long provideNetworkBytes() {
        return 0L;
    }

    protected long provideDiskBytes() {
        return 0L;
    }

    /**
     * Executes the profiling task. Requires that this instance is prepared.
     */
    protected abstract long executeOperator();

    protected static ChannelExecutor createChannelExecutor(final Collection<?> collection) {
        final ChannelExecutor channelExecutor = createChannelExecutor();
        channelExecutor.acceptCollection(collection);
        return channelExecutor;
    }

    protected static ChannelExecutor createChannelExecutor() {
        final ChannelDescriptor channelDescriptor = StreamChannel.DESCRIPTOR;
        final StreamChannel streamChannel = new StreamChannel(channelDescriptor, null);
        return JavaPlatform.getInstance().getChannelManager().createChannelExecutor(streamChannel);
    }

    public JavaExecutionOperator getOperator() {
        return this.operator;
    }

    public static void sleep(long millis) {
        try {
            System.out.printf("Sleeping for %s.\n", Formats.formatDuration(millis));
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * The result of a single profiling run.
     */
    public static class Result {

        private final List<Long> inputCardinalities;

        private final long outputCardinality;

        private final long diskBytes, networkBytes;

        private final long cpuCycles;

        public Result(List<Long> inputCardinalities, long outputCardinality, long diskBytes, long networkBytes, long cpuCycles) {
            this.inputCardinalities = inputCardinalities;
            this.outputCardinality = outputCardinality;
            this.diskBytes = diskBytes;
            this.networkBytes = networkBytes;
            this.cpuCycles = cpuCycles;
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
                    "inputCardinalities=" + this.inputCardinalities +
                    ", outputCardinality=" + this.outputCardinality +
                    ", diskBytes=" + this.diskBytes +
                    ", networkBytes=" + this.networkBytes +
                    ", cpuCycles=" + this.cpuCycles +
                    '}';
        }

        public String getCsvHeader() {
            return String.join(",", RheemCollections.map(this.inputCardinalities, (index, card) -> "input_card_" + index)) + "," +
                    "output_card," +
                    "disk," +
                    "network," +
                    "cpu";
        }

        public String toCsvString() {
            return String.join(",", RheemCollections.map(this.inputCardinalities, Object::toString)) + ","
                    + this.outputCardinality + ","
                    + this.diskBytes + ","
                    + this.networkBytes + ","
                    + this.cpuCycles;
        }
    }

}

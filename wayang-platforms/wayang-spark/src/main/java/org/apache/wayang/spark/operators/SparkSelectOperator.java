package org.apache.wayang.spark.operators;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.wayang.basic.operators.SelectOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.DatasetChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

/**
 * This class exploits Spark Dataset, in Spark a DataFrame is nothing but a Dataset<Row>
 */
import org.apache.spark.sql.Dataset;
import java.util.Collection;
import java.util.List;

public class SparkSelectOperator extends SelectOperator
        implements SparkExecutionOperator{

    public SparkSelectOperator(SelectOperator that) {
        super(that);
    }
    /**
     * evaluate function may work as follows (inspired ny SparkParquetSink and SparkFiterOperator)
     */
    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs,
                                                                                         ChannelInstance[] outputs, SparkExecutor sparkExecutor,
                                                                                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        //in Spark, a DF is nothing but a Dataset<Row>
        final Dataset<Row> inputDs = this.obtainDataset(inputs[0], sparkExecutor); //get the input DF from inputs, assuming I have obtainDataset
        Column[] columns = this.columns.stream()
                .map(functions::col)
                .toArray(Column[]::new);
        final Dataset<Row> outputDs = inputDs.select(columns);
        ((DatasetChannel.Instance) outputs[0]).accept(outputDs, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    /**
     * TODO...
     */

    @Override
    public boolean containsAction() {
        //this operator does not trigger the execution of the plan
        return false;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return List.of();
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return List.of();
    }
}

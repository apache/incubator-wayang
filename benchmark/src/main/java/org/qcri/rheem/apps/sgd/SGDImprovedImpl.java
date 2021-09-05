package org.qcri.rheem.apps.sgd;

import de.hpi.isg.profiledb.store.model.Experiment;
import org.qcri.rheem.api.DataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This class executes a stochastic gradient descent optimization on Rheem, just like {@link SGDImpl}. However,
 * it used the {@link org.qcri.rheem.basic.operators.MapPartitionsOperator} for performance improvements.
 */
public class SGDImprovedImpl {
    private final Configuration configuration;

    /**
     * {@link Plugin}s to use for the SGD.
     */
    private final List<Plugin> plugins;

    public SGDImprovedImpl(Configuration configuration, Plugin[] plugins) {
        this.configuration = configuration;
        this.plugins = Arrays.asList(plugins);
    }

    public double[] apply(String datasetUrl,
                          int datasetSize,
                          int features,
                          int maxIterations,
                          double accuracy,
                          int sampleSize) {
        return this.apply(datasetUrl, datasetSize, features, maxIterations, accuracy, sampleSize, null);
    }

    public double[] apply(String datasetUrl,
                          int datasetSize,
                          int features,
                          int maxIterations,
                          double accuracy,
                          int sampleSize,
                          Experiment experiment) {

        // Initialize the builder.
        RheemContext rheemContext = new RheemContext(this.configuration);
        for (Plugin plugin : this.plugins) {
            rheemContext.withPlugin(plugin);
        }
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);
        if (experiment != null) javaPlanBuilder.withExperiment(experiment);
        javaPlanBuilder.withUdfJarOf(this.getClass());

        // Create initial weights.
        List<double[]> weights = Arrays.asList(new double[features]);
        final DataQuantaBuilder<?, double[]> weightsBuilder = javaPlanBuilder
                .loadCollection(weights).withName("init weights");

        // Load and transform the data.
        final DataQuantaBuilder<?, double[]> transformBuilder = javaPlanBuilder
                .readTextFile(datasetUrl).withName("source")
                .mapPartitions(new TransformPerPartition(features)).withName("transform");


        // Do the SGD
        Collection<double[]> results  =
                weightsBuilder.doWhile(new LoopCondition(accuracy, maxIterations), w -> {
                    // Sample the data and update the weights.
                    DataQuantaBuilder<?, double[]> newWeightsDataset = transformBuilder
                        .sample(sampleSize).withDatasetSize(datasetSize).withBroadcast(w, "weights")
                        .mapPartitions(new ComputeLogisticGradientPerPartition(features)).withBroadcast(w, "weights").withName("compute")
                        .reduce(new Sum()).withName("reduce")
                        .map(new WeightsUpdate()).withBroadcast(w, "weights").withName("update");

                    // Calculate the convergence criterion.
                    DataQuantaBuilder<?, Tuple2<Double, Double>> convergenceDataset = newWeightsDataset
                            .map(new ComputeNorm()).withBroadcast(w, "weights");

                    return new Tuple<>(newWeightsDataset, convergenceDataset);
                }).withExpectedNumberOfIterations(maxIterations).collect();

        // Return the results.
        return RheemCollections.getSingleOrNull(results); // Support null for when execution is skipped.

    }
}

class TransformPerPartition implements FunctionDescriptor.SerializableFunction<Iterable<String>, Iterable<double[]>> {

    int features;

    public TransformPerPartition (int features) {
        this.features = features;
    }

    @Override
    public Iterable<double[]> apply(Iterable<String> lines) {
        List<double[]> list = new ArrayList<>();
        lines.forEach(line -> {
            String[] pointStr = line.split(" ");
            double[] point = new double[features+1];
            point[0] = Double.parseDouble(pointStr[0]);
            for (int i = 1; i < pointStr.length; i++) {
                if (pointStr[i].equals("")) {
                    continue;
                }
                String kv[] = pointStr[i].split(":", 2);
                point[Integer.parseInt(kv[0])-1] = Double.parseDouble(kv[1]);
            }
            list.add(point);
        });
        return list;
    }
}

class ComputeLogisticGradientPerPartition implements FunctionDescriptor.ExtendedSerializableFunction<Iterable<double[]>, Iterable<double[]>> {

    double[] weights;
    double[] sumGradOfPartition;
    int features;

    public ComputeLogisticGradientPerPartition(int features) {
        this.features = features;
        sumGradOfPartition = new double[features + 1]; //position 0 is for the count
    }

    @Override
    public Iterable<double[]> apply(Iterable<double[]> points) {
        List<double[]> list = new ArrayList<>(1);
        points.forEach(point -> {
            double dot = 0;
            for (int j = 0; j < weights.length; j++)
                dot += weights[j] * point[j + 1];
            for (int j = 0; j < weights.length; j++)
                sumGradOfPartition[j + 1] += ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

            sumGradOfPartition[0] += 1; //counter for the step size required in the update

        });
        list.add(sumGradOfPartition);
        return list;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        sumGradOfPartition = new double[features + 1];
    }
}

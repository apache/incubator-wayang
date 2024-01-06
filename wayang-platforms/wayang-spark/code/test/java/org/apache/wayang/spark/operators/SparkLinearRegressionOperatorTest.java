package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.model.LinearRegressionModel;
import org.apache.wayang.basic.operators.ModelTransformOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.model.SparkMLModel;
import org.apache.wayang.spark.operators.ml.SparkLinearRegressionOperator;
import org.apache.wayang.spark.operators.ml.SparkModelTransformOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SparkLinearRegressionOperatorTest extends SparkOperatorTestBase {

    // y = x1 + x2 + 1
    public static List<Tuple2<double[], Double>> trainingData = Arrays.asList(
            new Tuple2<>(new double[]{1, 1}, 3D),
            new Tuple2<>(new double[]{1, -1}, 1D),
            new Tuple2<>(new double[]{3, 2}, 6D)
    );

    public static List<double[]> inferenceData = Arrays.asList(
            new double[]{1, 2},
            new double[]{1, -2}
    );

    @Test
    public void testTraining() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(trainingData);
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        SparkLinearRegressionOperator linearRegressionOperator = new SparkLinearRegressionOperator(true);

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(linearRegressionOperator, inputs, outputs);

        // Verify the outcome.
        LinearRegressionModel model = output.<LinearRegressionModel>provideCollection().iterator().next();
        Assert.assertArrayEquals(new double[]{1, 1}, model.getCoefficients(), 1e-6);
        Assert.assertEquals(1, model.getIntercept(), 1e-6);
    }

    @Test
    public void testInference() {
        // Prepare test data.
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Collections.singletonList(
                // a mock model
                new SparkMLModel<double[], Double>() {
                    @Override
                    public JavaRDD<Tuple2<double[], Double>> transform(JavaRDD<double[]> input) {
                        return input.map(data -> new Tuple2<>(data, Arrays.stream(data).sum() + 1));
                    }
                }
        ));
        RddChannel.Instance input2 = this.createRddChannelInstance(inferenceData);
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkModelTransformOperator<double[], Double> transformOperator = new SparkModelTransformOperator<>(ModelTransformOperator.linearRegression());

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input1, input2};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(transformOperator, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<double[], Double>> results = output.<Tuple2<double[], Double>>provideRdd().collect();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(4, results.get(0).field1, 1e-6);
        Assert.assertEquals(0, results.get(1).field1, 1e-6);
    }
}

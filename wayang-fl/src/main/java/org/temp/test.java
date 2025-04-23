package org.temp;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml4all.abstraction.api.Compute;
import org.apache.wayang.ml4all.abstraction.api.Sample;
import org.apache.wayang.ml4all.abstraction.api.Transform;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;
import org.apache.wayang.ml4all.abstraction.plan.wrappers.ComputeWrapper;
import org.apache.wayang.ml4all.abstraction.plan.wrappers.TransformPerPartitionWrapper;
import org.apache.wayang.ml4all.algorithms.sgd.ComputeLogisticGradient;
import org.apache.wayang.ml4all.algorithms.sgd.SGDSample;
import org.client.FLClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class test {
    public static void main(String args[]){

        WayangContext wayangContext = new WayangContext(new Configuration()).withPlugin(Java.basicPlugin());
        JavaPlanBuilder pb = new JavaPlanBuilder(wayangContext)
                .withJobName("test-client"+"-job")
                .withUdfJarOf(FLClient.class);
//        List<Double> weights = new ArrayList<>(Collections.nCopies(29, 0.0));;
        double[] weights = new double[29];
        String inputFileUrl = "file:/Users/vedantaneogi/Downloads/higgs_part1.txt";
        int datasetSize = 29;
        ML4allModel model = new ML4allModel();
        model.put("weights", weights);
        ArrayList<ML4allModel> broadcastModel = new ArrayList<>(1);
        broadcastModel.add(model);
        // Step 1: Define ML operators
        Sample sampleOp = new SGDSample();
        Transform transformOp = new LibSVMTransform(29);
        Compute computeOp = new ComputeLogisticGradient();

        // Step 2: Create weight DataQuanta
        var weightsBuilder = pb
                .loadCollection(broadcastModel)
                .withName("model");

        // Step 3: Load dataset and apply transform
        DataQuantaBuilder transformBuilder = (DataQuantaBuilder) pb
                .readTextFile(inputFileUrl)
                .withName("source")
                .mapPartitions(new TransformPerPartitionWrapper(transformOp))
                .withName("transform");


//            Collection<?> parsedData = transformBuilder.collect();
//            for (Object row : parsedData) {
//                System.out.println(row);
//            }

        // Step 4: Sample, compute gradient, and broadcast weights
        DataQuantaBuilder result = (DataQuantaBuilder) transformBuilder
                .sample(sampleOp.sampleSize())
                .withSampleMethod(sampleOp.sampleMethod())
                .withDatasetSize(datasetSize)
                .map(new ComputeWrapper<>(computeOp))
                .withBroadcast(weightsBuilder, "model");
//
//        System.out.println(result.collect());
//        Collection<?> output = result.collect();
        for (Object o : result.collect()) {
            System.out.println("Type: " + o.getClass().getName());
            System.out.println("Value: " + o);
            for (Object idk : (double[]) o){
                System.out.println(idk);
            }
        }
    }
}

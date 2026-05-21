/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtLoggingLevel;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.providers.OrtCUDAProviderOptions;
import ai.onnxruntime.TensorInfo;
import ai.onnxruntime.OrtSession.Result;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OrtTensorDecoder;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.apache.wayang.ml.encoding.TreeDecoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.Logging;
import org.apache.wayang.ml.validation.*;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.HashSet;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.Arrays;
import java.time.Instant;
import java.time.Duration;

public class OrtMLModel {

    private static OrtMLModel INSTANCE;

    private OrtSession session;
    private OrtEnvironment env;
    private Configuration configuration;

    private final Map<String, OnnxTensor> inputMap = new HashMap<>();
    private final Set<String> requestedOutputs = new HashSet<>();

    public static OrtMLModel getInstance(Configuration configuration) throws OrtException {
        if (INSTANCE == null) {
            INSTANCE = new OrtMLModel(configuration);
        }

        return INSTANCE;
    }

    private OrtMLModel(Configuration configuration) throws OrtException {
        this.configuration = configuration;
        this.loadModel(configuration.getStringProperty("wayang.ml.model.file"));
    }

    private void loadModel(String filePath) throws OrtException {
        if (this.env == null) {
            this.env = OrtEnvironment.getEnvironment("org.apache.wayang.ml");
            this.env.setTelemetry(false);
        }

        if (this.session == null) {
            /*
            OrtCUDAProviderOptions cudaProviderOptions = new OrtCUDAProviderOptions(0);
            cudaProviderOptions.add("gpu_mem_limit","2147483648");
            cudaProviderOptions.add("arena_extend_strategy","kSameAsRequested");
            cudaProviderOptions.add("cudnn_conv_algo_search","DEFAULT");
            cudaProviderOptions.add("do_copy_in_default_stream","1");
            cudaProviderOptions.add("cudnn_conv_use_max_workspace","1");
            cudaProviderOptions.add("cudnn_conv1d_pad_to_nc1d","1");
            */

            OrtSession.SessionOptions options = new OrtSession.SessionOptions();

            options.setInterOpNumThreads(16);
            options.setIntraOpNumThreads(16);
            options.setDeterministicCompute(true);
            //options.setSessionLogLevel(OrtLoggingLevel.ORT_LOGGING_LEVEL_VERBOSE);
            //options.addCUDA(cudaProviderOptions);
            this.session = env.createSession(filePath, options);
        }
    }

    // Just here as placeholder
    public double runModel(long[] encoded) {
        return 0;
    }

    /**
     * Close the session after running, {@link #closeSession()}
     * @param encodedVector
     * @return NaN on error, and a predicted cost on any other value.
     * @throws OrtException
     */
    public double runModel(
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1
    ) throws OrtException {
        double costPrediction;

        Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();
        //
        //long[] input1Dims = new long[]{1, input1.field0.get(0).length, input1.field0.get(0)[0].length};
        //long[] input2Dims = new long[]{1, input1.field1.get(0).length, input1.field1.get(0)[0].length};

        Instant start = Instant.now();
        float[][][] inputValueStructure = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        long[][][] inputIndexStructure = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];

        //inputValueStructure = input1.field0.toArray(input1Left);
        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                inputValueStructure[0][i][j] = Long.valueOf(
                    input1.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            inputIndexStructure[0][i]  = input1.field1.get(0)[i];
        }

        OnnxTensor tensorValues = OnnxTensor.createTensor(env, inputValueStructure);
        OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);

        this.requestedOutputs.add("output");

        BiFunction<Result, String, Float> unwrapFunc = (r, s) -> {
            try {
                return ((float[]) r.get(s).get().getValue())[0];
            } catch (OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return Float.NaN;
            }
        };


        try (Result r = session.run(inputMap, requestedOutputs)) {
            costPrediction = unwrapFunc.apply(r, "output");
            Instant end = Instant.now();
            long execTime = Duration.between(start, end).toMillis();

            Logging.writeToFile(
                String.format("%d", execTime),
                this.configuration.getStringProperty("wayang.ml.optimizations.file")
            );
        } catch(Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }

        return costPrediction;
    }

    public int runPairwise(
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1,
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input2
    ) throws OrtException {


        Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();
        long[] input3Dims = ((TensorInfo) inputInfoList.get("input3").getInfo()).getShape();
        long[] input4Dims = ((TensorInfo) inputInfoList.get("input4").getInfo()).getShape();

        float[][][] inputValueStructure = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        long[][][] inputIndexStructure = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];
        float[][][] input2Left = new float[1][(int) input3Dims[1]][(int) input3Dims[2]];
        long[][][] input2Right = new long[1][(int) input4Dims[1]][(int) input4Dims[2]];

        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                inputValueStructure[0][i][j] = Long.valueOf(
                    input1.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            inputIndexStructure[0][i]  = input1.field1.get(0)[i];
        }

        for (int i = 0; i < input2.field0.get(0).length; i++) {
            for (int j = 0; j < input2.field0.get(0)[i].length; j++) {
                input2Left[0][i][j] = Long.valueOf(
                    input2.field0.get(0)[i][j]
                ).floatValue();
            }
        }

        for (int i = 0; i < input2.field1.get(0).length; i++) {
            input2Right[0][i]  = input2.field1.get(0)[i];
        }

        OnnxTensor tensorValues = OnnxTensor.createTensor(env, inputValueStructure);
        OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);
        OnnxTensor tensorTwoLeft = OnnxTensor.createTensor(env, input2Left);
        OnnxTensor tensorTwoRight = OnnxTensor.createTensor(env, input2Right);

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);
        this.inputMap.put("input3", tensorTwoLeft);
        this.inputMap.put("input4", tensorTwoRight);

        this.requestedOutputs.add("output");

        BiFunction<Result, String, Float[]> unwrapFunc = (r, s) -> {
            try {
                float[] result = ((float[]) r.get(s).get().getValue());
                Float[] convResult = new Float[result.length];

                for (int i = 0; i < result.length; i++) {
                    convResult[i] = result[i];
                }

                return convResult;
            } catch (OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                e.printStackTrace();
                return new Float[]{Float.NaN};
            }
        };

        try (Result r = session.run(this.inputMap, this.requestedOutputs)) {
            Float[] result = unwrapFunc.apply(r, "output");

            return Math.round(result[0]);
        } catch (OrtException e) {
            e.printStackTrace();

            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }
    }

    public Tuple<WayangPlan, TreeNode> runVAE(
        WayangPlan plan,
        TreeNode encoded
    ) throws OrtException {
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input = OrtTensorEncoder.encode(encoded);
        Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();

        System.out.println(encoded.toStringEncoding());

        //long[] input1Dims = new long[]{1, input.field0.get(0).length, input.field0.get(0)[0].length};
        //long[] input2Dims = new long[]{1, input.field1.get(0).length, input.field1.get(0)[0].length};

        System.out.println("Feature dims: " + Arrays.toString(input1Dims));
        System.out.println("Index dims: " + Arrays.toString(input2Dims));

        System.out.println("Tree size: " + encoded.size());

        int indexDims = encoded.size();
        long featureDims = input1Dims[1];
        Instant start = Instant.now();

        float[][] inputValueStructure = new float[(int) featureDims][(int) input1Dims[2]];
        //long[][][] inputIndexStructure = new long[1][indexDims][1];
        long[][][] inputIndexStructure = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];


        //inputValueStructure = input1.field0.toArray(input1Left);
        for (int i = 0; i < input.field0.get(0).length; i++) {
            for (int j = 0; j < input.field0.get(0)[i].length; j++) {
                // 0th entry as the model could take multiple trees
                // It only ever takes one here
                inputValueStructure[i][j] = Long.valueOf(
                    input.field0.get(0)[i][j]
                ).floatValue();
            }
        }
        /*
        long[][] inputIndexStructure = input.field1.get(0);
        */

        long[][] encoderIndexes = input.field1.get(0);

        long maxIndex = Arrays.stream(encoderIndexes)
                        .flatMapToLong(Arrays::stream)
                        .max()
                        .orElseThrow(() -> new IllegalArgumentException("Encoder indexes are empty"));

        assert maxIndex + 1 <= inputValueStructure[0].length : "There isn't a corresponding value for each index";

        for (int i = 0; i < input.field1.get(0).length; i++) {
            inputIndexStructure[0][i]  = input.field1.get(0)[i];
        }

        OnnxTensor tensorValues = OnnxTensor.createTensor(env, new float[][][]{inputValueStructure});
        OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);

        OrtTensorDecoder decoder = new OrtTensorDecoder();

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);

        this.requestedOutputs.add("output");

        BiFunction<Result, String, float[][][]> unwrapFunc = (r, s) -> {
            try {
                return ((float[][][]) r.get(s).get().getValue());
            } catch (OrtException e) {
                e.printStackTrace();
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return null;
            }
        };


        try (Result r = session.run(inputMap, requestedOutputs)) {
            float[][][] resultTensor = unwrapFunc.apply(r, "output");

            Instant end = Instant.now();
            long execTime = Duration.between(start, end).toMillis();

            Logging.writeToFile(
                String.format("Inference: %d", execTime),
                this.configuration.getStringProperty("wayang.ml.optimizations.file")
            );

            start = Instant.now();

            System.out.println("ResultTensor: " + resultTensor.length + ", " + resultTensor[0].length + ", " + resultTensor[0][0].length);

            long[][] platformChoices = PlatformChoiceValidator.validate(
                resultTensor,
                inputIndexStructure,
                encoded,
                new BitmaskValidationRule(),
                new OperatorValidationRule(),
                new PostgresSourceValidationRule()
            );

            System.out.println("Choices: " + Arrays.deepToString(platformChoices));

            int valueDim = resultTensor[0][0].length;
            int indexDim = input.field1.get(0).length;

            // Only handle one tree
            //assert valueDim == indexDim : "Index dim " + indexDim + " != " + valueDim + " valueDim";

            ArrayList<long[][]> mlResult = new ArrayList<long[][]>();
            mlResult.add(platformChoices);

            Tuple<ArrayList<long[][]>, ArrayList<long[][]>> decoderInput = new Tuple<>(mlResult, input.field1);
            end = Instant.now();
            execTime = Duration.between(start, end).toMillis();

            Logging.writeToFile(
                String.format("Unpacking: %d", execTime),
                this.configuration.getStringProperty("wayang.ml.optimizations.file")
            );

            start = Instant.now();
            TreeNode decoded = decoder.decode(decoderInput);

            //decoded.softmax();
            end = Instant.now();

            execTime = Duration.between(start, end).toMillis();
            Logging.writeToFile(
                String.format("Decoding: %d", execTime),
                this.configuration.getStringProperty("wayang.ml.optimizations.file")
            );
            // Now set the platforms on the wayangPlan
            start = Instant.now();

            assert decoded.size() == encoded.size() : "Mismatch in Decode and Encode tree sizes";

            TreeNode reconstructed = encoded.withPlatformChoicesFrom(decoded);
            WayangPlan decodedPlan = TreeDecoder.decode(reconstructed);
            end = Instant.now();

            execTime = Duration.between(start, end).toMillis();
            /*Logging.writeToFile(
                String.format("Reconstruction: %d", execTime),
                this.configuration.getStringProperty("wayang.ml.optimizations.file")
            )*/;



            return new Tuple<WayangPlan, TreeNode>(decodedPlan, reconstructed);
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
            //return new Tuple<WayangPlan, TreeNode>(plan, encoded);
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
            this.closeSession();
        }
    }

    /**
     * Closes the OrtModel resource, relinquishing any underlying resources.
     * @throws OrtException
     */
    public void closeSession() throws OrtException {
        this.session.close();
        this.env.close();
    }
}

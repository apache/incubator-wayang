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

package org.apache.wayang.ml.encoding;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.util.Logging;
import org.apache.wayang.ml.validation.BitmaskValidationRule;
import org.apache.wayang.ml.validation.OperatorValidationRule;
import org.apache.wayang.ml.validation.PlatformChoiceValidator;
import org.apache.wayang.ml.validation.PostgresSourceValidationRule;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtSession.Result;
import ai.onnxruntime.TensorInfo;

public class OrtMLModel {
    private static OrtMLModel INSTANCE;

    public static OrtMLModel getInstance(final Configuration configuration) throws OrtException {
        if (INSTANCE == null) {
            INSTANCE = new OrtMLModel(configuration);
        }

        return INSTANCE;
    }

    private OrtSession session;
    private OrtEnvironment env;

    private final Configuration configuration;
    private final Map<String, OnnxTensor> inputMap = new HashMap<>();

    private final Set<String> requestedOutputs = new HashSet<>();

    private OrtMLModel(final Configuration configuration) throws OrtException {
        this.configuration = configuration;
        this.loadModel(configuration.getStringProperty("wayang.ml.model.file"));
    }

    /**
     * placeholder
     * @param encoded
     * @return
     */
    public double runModel(final long[] encoded) {
        return 0;
    }

    /**
     * Close the session after running, {@link #closeSession()}
     * 
     * @param encodedVector
     * @return NaN on error, and a predicted cost on any other value.
     * @throws OrtException
     */
    public double runModel(final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1) throws OrtException {
        double costPrediction;

        final Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        final long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        final long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();

        final Instant start = Instant.now();
        final float[][][] inputValueStructure = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        final long[][][] inputIndexStructure = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];

        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                inputValueStructure[0][i][j] = Long.valueOf(input1.field0.get(0)[i][j]).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            inputIndexStructure[0][i] = input1.field1.get(0)[i];
        }

        final OnnxTensor tensorValues = OnnxTensor.createTensor(env, inputValueStructure);
        final OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);

        this.requestedOutputs.add("output");

        final BiFunction<Result, String, Float> unwrapFunc = (r, s) -> {
            try {
                return ((float[]) r.get(s).get().getValue())[0];
            } catch (final OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return Float.NaN;
            }
        };

        try (Result r = session.run(inputMap, requestedOutputs)) {
            costPrediction = unwrapFunc.apply(r, "output");
            final Instant end = Instant.now();
            final long execTime = Duration.between(start, end).toMillis();

            Logging.writeToFile(String.format("%d", execTime),
                    this.configuration.getStringProperty("wayang.ml.optimizations.file"));
        } catch (final Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }

        return costPrediction;
    }

    public int runPairwise(final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input1,
            final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input2) throws OrtException {

        final Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        final long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        final long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();
        final long[] input3Dims = ((TensorInfo) inputInfoList.get("input3").getInfo()).getShape();
        final long[] input4Dims = ((TensorInfo) inputInfoList.get("input4").getInfo()).getShape();

        final float[][][] inputValueStructure = new float[1][(int) input1Dims[1]][(int) input1Dims[2]];
        final long[][][] inputIndexStructure = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];
        final float[][][] input2Left = new float[1][(int) input3Dims[1]][(int) input3Dims[2]];
        final long[][][] input2Right = new long[1][(int) input4Dims[1]][(int) input4Dims[2]];

        for (int i = 0; i < input1.field0.get(0).length; i++) {
            for (int j = 0; j < input1.field0.get(0)[i].length; j++) {
                inputValueStructure[0][i][j] = Long.valueOf(input1.field0.get(0)[i][j]).floatValue();
            }
        }

        for (int i = 0; i < input1.field1.get(0).length; i++) {
            inputIndexStructure[0][i] = input1.field1.get(0)[i];
        }

        for (int i = 0; i < input2.field0.get(0).length; i++) {
            for (int j = 0; j < input2.field0.get(0)[i].length; j++) {
                input2Left[0][i][j] = Long.valueOf(input2.field0.get(0)[i][j]).floatValue();
            }
        }

        for (int i = 0; i < input2.field1.get(0).length; i++) {
            input2Right[0][i] = input2.field1.get(0)[i];
        }

        final OnnxTensor tensorValues = OnnxTensor.createTensor(env, inputValueStructure);
        final OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);
        final OnnxTensor tensorTwoLeft = OnnxTensor.createTensor(env, input2Left);
        final OnnxTensor tensorTwoRight = OnnxTensor.createTensor(env, input2Right);

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);
        this.inputMap.put("input3", tensorTwoLeft);
        this.inputMap.put("input4", tensorTwoRight);

        this.requestedOutputs.add("output");

        final BiFunction<Result, String, Float[]> unwrapFunc = (r, s) -> {
            try {
                final float[] result = ((float[]) r.get(s).get().getValue());
                final Float[] convResult = new Float[result.length];

                for (int i = 0; i < result.length; i++) {
                    convResult[i] = result[i];
                }

                return convResult;
            } catch (final OrtException e) {
                this.inputMap.clear();
                this.requestedOutputs.clear();

                e.printStackTrace();
                return new Float[] { Float.NaN };
            }
        };

        try (Result r = session.run(this.inputMap, this.requestedOutputs)) {
            final Float[] result = unwrapFunc.apply(r, "output");

            return Math.round(result[0]);
        } catch (final OrtException e) {
            e.printStackTrace();

            return 0;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
        }
    }

    public Tuple<WayangPlan, TreeNode> runVAE(final WayangPlan plan, final TreeNode encoded) throws OrtException {
        final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input = OrtTensorEncoder.encode(encoded);
        final Map<String, NodeInfo> inputInfoList = this.session.getInputInfo();
        final long[] input1Dims = ((TensorInfo) inputInfoList.get("input1").getInfo()).getShape();
        final long[] input2Dims = ((TensorInfo) inputInfoList.get("input2").getInfo()).getShape();

        System.out.println(encoded.toStringEncoding());
        System.out.println("Feature dims: " + Arrays.toString(input1Dims));
        System.out.println("Index dims: " + Arrays.toString(input2Dims));
        System.out.println("Tree size: " + encoded.size());

        final long featureDims = input1Dims[1];
        Instant start = Instant.now();

        final float[][] inputValueStructure = new float[(int) featureDims][(int) input1Dims[2]];
        final long[][][] inputIndexStructure = new long[1][(int) input2Dims[1]][(int) input2Dims[2]];

        for (int i = 0; i < input.field0.get(0).length; i++) {
            for (int j = 0; j < input.field0.get(0)[i].length; j++) {
                inputValueStructure[i][j] = Long.valueOf(input.field0.get(0)[i][j]).floatValue();
            }
        }

        final long[][] encoderIndexes = input.field1.get(0);

        final long maxIndex = Arrays.stream(encoderIndexes).flatMapToLong(Arrays::stream).max()
                .orElseThrow(() -> new IllegalArgumentException("Encoder indexes are empty"));

        assert maxIndex + 1 <= inputValueStructure[0].length : "There isn't a corresponding value for each index";

        for (int i = 0; i < input.field1.get(0).length; i++) {
            inputIndexStructure[0][i] = input.field1.get(0)[i];
        }

        final OnnxTensor tensorValues = OnnxTensor.createTensor(env, new float[][][] { inputValueStructure });
        final OnnxTensor tensorIndexes = OnnxTensor.createTensor(env, inputIndexStructure);

        final OrtTensorDecoder decoder = new OrtTensorDecoder();

        this.inputMap.put("input1", tensorValues);
        this.inputMap.put("input2", tensorIndexes);

        this.requestedOutputs.add("output");

        final BiFunction<Result, String, float[][][]> unwrapFunc = (r, s) -> {
            try {
                return ((float[][][]) r.get(s).get().getValue());
            } catch (final OrtException e) {
                e.printStackTrace();
                this.inputMap.clear();
                this.requestedOutputs.clear();

                return null;
            }
        };

        try (Result r = session.run(inputMap, requestedOutputs)) {
            final float[][][] resultTensor = unwrapFunc.apply(r, "output");
            Instant end = Instant.now();
            long execTime = Duration.between(start, end).toMillis();
            Logging.writeToFile(String.format("Inference: %d", execTime),
                    this.configuration.getStringProperty("wayang.ml.optimizations.file"));

            start = Instant.now();
            System.out.println("ResultTensor: " + resultTensor.length + ", " + resultTensor[0].length + ", "
                    + resultTensor[0][0].length);

            final long[][] platformChoices = PlatformChoiceValidator.validate(resultTensor, inputIndexStructure, encoded,
                    new BitmaskValidationRule(), new OperatorValidationRule(), new PostgresSourceValidationRule());

            System.out.println("Choices: " + Arrays.deepToString(platformChoices));

            final ArrayList<long[][]> mlResult = new ArrayList<long[][]>();
            mlResult.add(platformChoices);

            final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> decoderInput = new Tuple<>(mlResult, input.field1);
            end = Instant.now();
            execTime = Duration.between(start, end).toMillis();

            Logging.writeToFile(String.format("Unpacking: %d", execTime),
                    this.configuration.getStringProperty("wayang.ml.optimizations.file"));

            start = Instant.now();
            final TreeNode decoded = decoder.decode(decoderInput);
            end = Instant.now();
            execTime = Duration.between(start, end).toMillis();
            Logging.writeToFile(String.format("Decoding: %d", execTime),
                    this.configuration.getStringProperty("wayang.ml.optimizations.file"));

            start = Instant.now();
            assert decoded.size() == encoded.size() : "Mismatch in Decode and Encode tree sizes";
            final TreeNode reconstructed = encoded.withPlatformChoicesFrom(decoded);
            final WayangPlan decodedPlan = TreeDecoder.decode(reconstructed);
            end = Instant.now();
            execTime = Duration.between(start, end).toMillis();

            return new Tuple<WayangPlan, TreeNode>(decodedPlan, reconstructed);
        } catch (final Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            this.inputMap.clear();
            this.requestedOutputs.clear();
            this.closeSession();
        }
    }

    /**
     * Closes the OrtModel resource, relinquishing any underlying resources.
     * 
     * @throws OrtException
     */
    public void closeSession() throws OrtException {
        this.session.close();
        this.env.close();
    }

    private void loadModel(final String filePath) throws OrtException {
        if (this.env == null) {
            this.env = OrtEnvironment.getEnvironment("org.apache.wayang.ml");
            this.env.setTelemetry(false);
        }

        if (this.session == null) {
            final OrtSession.SessionOptions options = new OrtSession.SessionOptions();

            options.setInterOpNumThreads(16);
            options.setIntraOpNumThreads(16);
            options.setDeterministicCompute(true);

            this.session = env.createSession(filePath, options);
        }
    }
}

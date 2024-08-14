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

package org.apache.wayang.tensorflow.model;

import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.Input;
import org.apache.wayang.basic.model.op.Op;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.tensorflow.Graph;
import org.tensorflow.Operand;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.*;
import org.tensorflow.ndarray.index.Indices;
import org.tensorflow.op.Ops;
import org.tensorflow.types.*;
import org.tensorflow.types.family.TType;

import java.util.*;

public class TensorflowModel extends DLModel implements AutoCloseable {
    private final Op criterion;
    private final Optimizer optimizer;
    private final Op accuracyCalculation;

    private final Graph graph;
    private final Ops tf;
    private final Session session;
    private final Map<String, Operand<?>> opMap;
    private final org.tensorflow.op.Op minimize;

    public TensorflowModel(DLModel model, Op criterion, Optimizer optimizer, Op accuracyCalculation) {
        super(model.getOut());
        this.criterion = criterion;
        this.optimizer = optimizer;
        this.accuracyCalculation = accuracyCalculation;

        this.graph = new Graph();
        this.tf = Ops.create(graph);
        this.session = new Session(graph);
        this.opMap = new HashMap<>();

        connect(criterion);
        compile(criterion);
        if (accuracyCalculation != null) {
            connect(accuracyCalculation);
            compile(accuracyCalculation);
        }

        this.minimize = Convertor.convert(graph, optimizer).minimize(opMap.get(criterion.getName()));
    }

    private void connect(Op op) {
        Deque<List<Op>> deque = new LinkedList<>();
        deque.addLast(op.getFromList());
        boolean changeInput = false;
        while (!deque.isEmpty() && !changeInput) {
            List<Op> fromList = deque.pollFirst();
            for (int i = 0; i < fromList.size(); i++) {
                if (fromList.get(i).getName().equals(Input.Type.PREDICTED.getName())) {
                    fromList.set(i, out);
                    changeInput = true;
                    break;
                }
                deque.addLast(fromList.get(i).getFromList());
            }
        }
        if (!changeInput) {
            throw new RuntimeException("Op " + op.getName() + " operator must start with a Input named '__PREDICTED__'");
        }
    }

    private Operand<?> compile(Op op) {
        Operand[] array = op.getFromList().stream().map(e -> {
            Operand<?> operand = this.opMap.get(e.getName());
            if (operand == null) {
                operand = compile(e);
            }
            return operand;
        }).toArray(Operand[]::new);
        final Operand<?> ret = Convertor.convert(tf, op, array);
        this.opMap.put(op.getName(), ret);
        return ret;
    }

    public
    <XT extends NdArray<?>, YT extends NdArray<?>>
    void train(XT x, YT y, int epoch, int batchSize) {
        System.out.println("Start training:");

        for (int i = 0; i < epoch; i++) {
            int n = (int) y.shape().size(0);
            for (int start = 0; start < n; start += batchSize) {
                int end = Math.min(start + batchSize, n);
                NdArray<?> x_ = x.slice(Indices.slice(start, end));
                NdArray<?> y_ = y.slice(Indices.slice(start, end));
                try (
                        Tensor tx = Convertor.ndArrayToTensor(x_);
                        Tensor ty = Convertor.ndArrayToTensor(y_);
                ) {
                    Session.Runner runner = session.runner()
                            .feed(Input.Type.FEATURES.getName(), tx)
                            .feed(Input.Type.LABEL.getName(), ty)
                            .addTarget(minimize)
                            .fetch(criterion.getName());
                    if (accuracyCalculation != null) {
                        runner.fetch(accuracyCalculation.getName());
                    }
                    List<Tensor> ret = runner.run();
                    try (TFloat32 loss = (TFloat32) ret.get(0)) {
                        System.out.printf("[epoch %d, batch %d] loss: %f ", i + 1, start / batchSize + 1, loss.getFloat());
                    }
                    if (accuracyCalculation != null) {
                        try (TFloat32 acc = (TFloat32) ret.get(1)) {
                            System.out.printf("accuracy: %f ", acc.getFloat());
                        }
                    }
                    System.out.println();
                }
            }
        }

        System.out.println("Finish training.\n");
    }

    public
    <XT extends NdArray<?>, PT extends NdArray<?> & TType>
    PT predict(XT x) {
        try (Tensor tx = Convertor.ndArrayToTensor(x)) {
            Tensor predicted = session.runner()
                    .feed(Input.Type.FEATURES.getName(), tx)
                    .fetch(out.getName())
                    .run()
                    .get(0);
            return (PT) predicted;
        }
    }

    public Op getCriterion() {
        return criterion;
    }

    public Optimizer getOptimizer() {
        return optimizer;
    }

    public Op getAccuracyCalculation() {
        return accuracyCalculation;
    }

    @Override
    public void close() {
        session.close();
        graph.close();
    }
}

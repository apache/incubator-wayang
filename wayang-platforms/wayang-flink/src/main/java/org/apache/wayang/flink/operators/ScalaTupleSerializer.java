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

package org.apache.wayang.flink.operators;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import scala.Product;

public class ScalaTupleSerializer extends Serializer<Product> {

    public ScalaTupleSerializer() {
        setImmutable(true); // Scala Tuples are immutable
    }

    @Override
    public void write(Kryo kryo, Output output, Product tuple) {
        int size = tuple.productArity(); // Get tuple size (e.g., 2 for Tuple2, 5 for Tuple5)
        output.writeInt(size); // Write tuple size first

        // Serialize each element
        for (int i = 0; i < size; i++) {
            kryo.writeClassAndObject(output, tuple.productElement(i));
        }
    }

    @Override
    public Product read(Kryo kryo, Input input, Class<Product> type) {
        int size = input.readInt(); // Read tuple size

        Object[] elements = new Object[size];
        for (int i = 0; i < size; i++) {
            elements[i] = kryo.readClassAndObject(input);
        }

        // Dynamically reconstruct the tuple using Scala reflection
        try {
            Class<?> tupleClass = Class.forName("scala.Tuple" + size);
            return (Product) tupleClass.getConstructors()[0].newInstance(elements);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize tuple of size " + size, e);
        }
    }
}

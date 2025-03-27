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
import scala.Tuple4;

public class Tuple4Serializer<A, B, C, D> extends Serializer<Tuple4<A, B, C, D>> {

    public Tuple4Serializer() {
        setImmutable(true); // Scala tuples are immutable
    }

    @Override
    public void write(Kryo kryo, Output output, Tuple4<A, B, C, D> object) {
        kryo.writeClassAndObject(output, object._1());
        kryo.writeClassAndObject(output, object._2());
        kryo.writeClassAndObject(output, object._3());
        kryo.writeClassAndObject(output, object._4());
    }

    @Override
    public Tuple4<A, B, C, D> read(Kryo kryo, Input input, Class<Tuple4<A, B, C, D>> type) {
        A first = (A) kryo.readClassAndObject(input);
        B second = (B) kryo.readClassAndObject(input);
        C third = (C) kryo.readClassAndObject(input);
        D fourth = (D) kryo.readClassAndObject(input);
        return new Tuple4<>(first, second, third, fourth);
    }
}

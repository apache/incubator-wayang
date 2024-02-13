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

package org.apache.wayang.flink.compiler.criterion;


import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
/**
 * Implementation of {@link Value} of flink for use in Wayang
 */
public class WayangValue<T> implements Value {
    private static final int HIGH_BIT = 0x1 << 7;

    private static final int HIGH_BIT2 = 0x1 << 13;

    private static final int HIGH_BIT2_MASK = 0x3 << 6;

    private T data;

    public WayangValue(){
        this.data = null;
    }

    public WayangValue(T element){
        this.data = element;
    }

    @Override
    public void write(DataOutputView dataOutputView) throws IOException {
        byte[] element = convertToByte();
        // write the length, variable-length encoded
        int len = element.length;
        while (len >= HIGH_BIT) {
            dataOutputView.write(len | HIGH_BIT);
            len >>>= 7;
        }
        dataOutputView.write(len);
        dataOutputView.write(element);


    }

    @Override
    public void read(DataInputView dataInputView) throws IOException {

        int len = dataInputView.readUnsignedByte();

        if (len >= HIGH_BIT) {
            int shift = 7;
            int curr;
            len = len & 0x7f;
            while ((curr = dataInputView.readUnsignedByte()) >= HIGH_BIT) {
                len |= (curr & 0x7f) << shift;
                shift += 7;
            }
            len |= curr << shift;
        }

        byte[] array = new byte[len];

        dataInputView.read(array);

        this.data = convertToObject(array);
    }

    public byte[] convertToByte(){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] result = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this.data);
            out.flush();
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    public T convertToObject(byte[] array){
        ByteArrayInputStream bis = new ByteArrayInputStream(array);
        ObjectInput in = null;
        T object = null;
        try {
            in = new ObjectInputStream(bis);
            object = (T) in.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return object;
    }

    public String toString(){
        if(this.data == null){
            return "null";
        }
        return this.data.toString();
    }

    public T get(){
        return this.data;
    }
}

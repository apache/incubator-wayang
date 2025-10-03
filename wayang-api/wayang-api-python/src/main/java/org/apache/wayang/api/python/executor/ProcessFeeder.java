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

package org.apache.wayang.api.python.executor;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.apache.wayang.core.api.exception.WayangException;

import com.google.protobuf.ByteString;

public class ProcessFeeder<Input, Output> {

    // TODO add to a config file
    static final int END_OF_DATA_SECTION = -1;
    static final int NULL = -5;

    private final Socket socket;
    private final ByteString serializedUDF;
    private final Iterable<Input> input;

    public ProcessFeeder(
            final Socket socket,
            final ByteString serializedUDF,
            final Iterable<Input> input) {

        if (input == null)
            throw new WayangException("Nothing to process with Python API");

        this.socket = socket;
        this.serializedUDF = serializedUDF;
        this.input = input;

    }

    public void send() {
        try {
            // TODO use config buffer size
            final int BUFFER_SIZE = 65536;

            final BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE);
            final DataOutputStream dataOut = new DataOutputStream(stream);

            writeUDF(serializedUDF, dataOut);
            this.writeIteratorToStream(input.iterator(), dataOut);
            dataOut.writeInt(END_OF_DATA_SECTION);
            dataOut.flush();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public void writeUDF(final ByteString serializedUDF, final DataOutputStream dataOut) {
        writeBytes(serializedUDF.toByteArray(), dataOut);
    }

    public void writeIteratorToStream(final Iterator<Input> iter, final DataOutputStream dataOut) {
        for (final Iterator<Input> it = iter; it.hasNext();) {
            final Input elem = it.next();
            write(elem, dataOut);
        }
    }

    /* TODO Missing case PortableDataStream */
    public void write(final Object obj, final DataOutputStream dataOut) {
        try {

            if (obj == null)
                dataOut.writeInt(ProcessFeeder.NULL);

            /**
             * Byte Array cases
             */
            else if (obj instanceof Byte[] || obj instanceof byte[]) {
                writeBytes(obj, dataOut);
            }
            /**
             * String case
             */
            else if (obj instanceof final String str) {
                writeUTF(str, dataOut);
            }

            // TODO: Properly type this in the future
            else if (obj instanceof Object) {
                writeUTF(String.valueOf(obj), dataOut);
            }

            /**
             * Key, Value case
             */
            else if (obj instanceof final Map.Entry<?,?> entry) {
                writeKeyValue(entry, dataOut);
            }

            else {
                throw new WayangException("Unexpected element type " + obj.getClass());
            }

        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public void writeBytes(final Object obj, final DataOutputStream dataOut) {

        try {

            if (obj instanceof final Byte[] objBytes) {

                final int length = objBytes.length;

                final byte[] bytes = new byte[length];
                int j = 0;

                // Unboxing Byte values. (Byte[] to byte[])
                for (final Byte b : objBytes)
                    bytes[j++] = b.byteValue();

                dataOut.writeInt(length);
                dataOut.write(bytes);

            } else if (obj instanceof final byte[] objBytes) {

                dataOut.writeInt(objBytes.length);
                dataOut.write(objBytes);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public void writeUTF(final String str, final DataOutputStream dataOut) {

        final byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        try {
            dataOut.writeInt(bytes.length);
            dataOut.write(bytes);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public void writeKeyValue(final Map.Entry<?, ?> obj, final DataOutputStream dataOut) {
        write(obj.getKey(), dataOut);
        write(obj.getValue(), dataOut);
    }
}

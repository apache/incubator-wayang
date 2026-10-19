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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JavaCSVFileSourceTest extends JavaExecutionOperatorTestBase {

    @TempDir
    Path directory;

    private List<Record> read(String csv, List<CsvType> types, char separator) throws Exception {
        Path file = directory.resolve("input.csv");
        Files.writeString(file, csv);
        JavaCSVFileSource<Record> source = new JavaCSVFileSource<>(
                file.toUri().toString(), DataSetType.createDefault(Record.class), types, separator);
        JavaChannelInstance[] outputs = {createStreamChannelInstance()};
        evaluate(source, new JavaChannelInstance[0], outputs);
        try (Stream<Record> records = outputs[0].provideStream()) {
            return records.collect(Collectors.toList());
        }
    }

    @Test
    void readsTypedRecordsWithCustomSeparatorAndQuotedFields() throws Exception {
        List<Record> records = read("id:int,name:string,price:decimal\n1;\"a;b\";12.345\n",
                List.of(CsvType.INTEGER, CsvType.STRING, CsvType.decimal(5, 2)), ';');
        assertEquals(List.of(new Record(1, "a;b", new BigDecimal("12.35"))), records);
    }

    @Test
    void rejectsDecimalOverflowAfterRounding() {
        assertThrows(IllegalArgumentException.class, () -> read("price:decimal\n99.995\n",
                List.of(CsvType.decimal(4, 2)), ';'));
    }

    @Test
    void rejectsRowsWithMissingFields() {
        assertThrows(IllegalStateException.class, () -> read("id:int,name:string\n1\n",
                List.of(CsvType.INTEGER, CsvType.STRING), ';'));
    }

    @Test
    void retainsTypedHeaderValidation() {
        assertThrows(IllegalStateException.class, () -> read("id,name\n1;Alice\n",
                List.of(CsvType.INTEGER, CsvType.STRING), ';'));
    }

    @Test
    void rejectsEmptyFiles() {
        assertThrows(IllegalStateException.class, () -> read("", List.of(CsvType.INTEGER), ';'));
    }
}

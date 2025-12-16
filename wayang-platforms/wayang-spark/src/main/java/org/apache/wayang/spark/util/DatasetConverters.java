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

package org.apache.wayang.spark.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.types.DataSetType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods to convert {@link Record}-backed RDDs into Spark {@link Dataset}s.
 */
public final class DatasetConverters {

    private static final int SCHEMA_SAMPLE_SIZE = 50;

    private DatasetConverters() {
    }

    /**
     * Convert an RDD of {@link Record}s into a Spark {@link Dataset}.
     *
     * @param records      the records to convert
     * @param dataSetType  type information about the records (field names etc.)
     * @param sparkSession the {@link SparkSession} used to create the {@link Dataset}
     * @return a {@link Dataset} view over {@code records}
     */
    public static Dataset<Row> recordsToDataset(JavaRDD<Record> records,
                                                DataSetType<Record> dataSetType,
                                                SparkSession sparkSession) {
        StructType schema = deriveSchema(records, dataSetType);
        JavaRDD<Row> rows = records.map(record -> RowFactory.create(record.getValues()));
        return sparkSession.createDataFrame(rows, schema);
    }

    private static StructType deriveSchema(JavaRDD<Record> rdd, DataSetType<Record> dataSetType) {
        List<Record> samples = rdd.take(SCHEMA_SAMPLE_SIZE);
        RecordType recordType = extractRecordType(dataSetType);
        String[] fieldNames = resolveFieldNames(samples, recordType);

        List<StructField> fields = new ArrayList<>(fieldNames.length);
        for (int column = 0; column < fieldNames.length; column++) {
            DataType dataType = inferColumnType(samples, column);
            fields.add(DataTypes.createStructField(fieldNames[column], dataType, true));
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    private static RecordType extractRecordType(DataSetType<Record> dataSetType) {
        if (dataSetType == null || dataSetType.getDataUnitType() == null) {
            return null;
        }
        if (dataSetType.getDataUnitType() instanceof RecordType) {
            return (RecordType) dataSetType.getDataUnitType();
        }
        if (dataSetType.getDataUnitType().toBasicDataUnitType() instanceof RecordType) {
            return (RecordType) dataSetType.getDataUnitType().toBasicDataUnitType();
        }
        return null;
    }

    private static String[] resolveFieldNames(List<Record> samples, RecordType recordType) {
        if (recordType != null && recordType.getFieldNames() != null && recordType.getFieldNames().length > 0) {
            return recordType.getFieldNames();
        }
        Record sample = samples.isEmpty() ? null : samples.get(0);
        int numFields = sample == null ? 0 : sample.size();
        String[] names = new String[numFields];
        for (int index = 0; index < numFields; index++) {
            names[index] = "field" + index;
        }
        return names;
    }

    private static DataType inferColumnType(List<Record> samples, int columnIndex) {
        for (Record sample : samples) {
            if (sample == null || columnIndex >= sample.size()) {
                continue;
            }
            Object value = sample.getField(columnIndex);
            if (value == null) {
                continue;
            }
            DataType dataType = toSparkType(value);
            if (dataType != null) {
                return dataType;
            }
        }
        return DataTypes.StringType;
    }

    private static DataType toSparkType(Object value) {
        if (value instanceof String || value instanceof Character) {
            return DataTypes.StringType;
        } else if (value instanceof Integer) {
            return DataTypes.IntegerType;
        } else if (value instanceof Long) {
            return DataTypes.LongType;
        } else if (value instanceof Short) {
            return DataTypes.ShortType;
        } else if (value instanceof Byte) {
            return DataTypes.ByteType;
        } else if (value instanceof Double) {
            return DataTypes.DoubleType;
        } else if (value instanceof Float) {
            return DataTypes.FloatType;
        } else if (value instanceof Boolean) {
            return DataTypes.BooleanType;
        } else if (value instanceof Timestamp) {
            return DataTypes.TimestampType;
        } else if (value instanceof java.sql.Date) {
            return DataTypes.DateType;
        } else if (value instanceof byte[]) {
            return DataTypes.BinaryType;
        } else if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            int precision = Math.min(38, Math.max(decimal.precision(), decimal.scale()));
            int scale = Math.max(0, decimal.scale());
            return DataTypes.createDecimalType(precision, scale);
        } else if (value instanceof BigInteger) {
            BigInteger bigInteger = (BigInteger) value;
            int precision = Math.min(38, bigInteger.toString().length());
            return DataTypes.createDecimalType(precision, 0);
        }
        return null;
    }
}

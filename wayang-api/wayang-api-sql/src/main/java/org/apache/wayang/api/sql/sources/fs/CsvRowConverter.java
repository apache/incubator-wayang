/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.sources.fs;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.log4j.LogMF;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Based on Calcite's CSV enumerator.
 * TODO: handle different variants
 */
public class CsvRowConverter {

    private static final Logger logger = LogManager.getLogger(CsvRowConverter.class);

    private static final CSVParser parser;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);

        parser = new CSVParser();
    }


    public static Object convert(RelDataType fieldType, String string) {
        if (fieldType == null || string == null) {
            return string;
        }
        switch (fieldType.getSqlTypeName()) {
            case BOOLEAN:
                if (string.length() == 0) {
                    return null;
                }
                return Boolean.parseBoolean(string);
            case TINYINT:
                if (string.length() == 0) {
                    return null;
                }
                return Byte.parseByte(string);
            case SMALLINT:
                if (string.length() == 0) {
                    return null;
                }
                return Short.parseShort(string);
            case INTEGER:
                if (string.length() == 0) {
                    return null;
                }
                return Integer.parseInt(string);
            case BIGINT:
                if (string.length() == 0) {
                    return null;
                }
                return Long.parseLong(string);
            case FLOAT:
                if (string.length() == 0) {
                    return null;
                }
                return Float.parseFloat(string);
            case DOUBLE:
                if (string.length() == 0) {
                    return null;
                }
                return Double.parseDouble(string);
            case DECIMAL:
                if (string.length() == 0) {
                    return null;
                }
                return parseDecimal(fieldType.getPrecision(), fieldType.getScale(), string);
            case DATE:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_DATE.parse(string);
                    return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                } catch (ParseException e) {
                    return null;
                }
            case TIME:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIME.parse(string);
                    return (int) date.getTime();
                } catch (ParseException e) {
                    return null;
                }
            case TIMESTAMP:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                    return date.getTime();
                } catch (ParseException e) {
                    return null;
                }
            case VARCHAR:
            default:
                return string;
        }
    }

    private static BigDecimal parseDecimal(int precision, int scale, String string) {

        BigDecimal result = new BigDecimal(string);
        // If the parsed value has more fractional digits than the specified scale, round ties away
        // from 0.
        if (result.scale() > scale) {

            logger.warn(
                    "Decimal value {} exceeds declared scale ({}). Performing rounding to keep the "
                            + "first {} fractional digits.",
                    result, scale, scale);
            result = result.setScale(scale, RoundingMode.HALF_UP);
        }
        // Throws an exception if the parsed value has more digits to the left of the decimal point
        // than the specified value.
        if (result.precision() - result.scale() > precision - scale) {
            throw new IllegalArgumentException(String
                    .format(Locale.ROOT, "Decimal value %s exceeds declared precision (%d) and scale (%d).",
                            result, precision, scale));
        }
        return result;
    }


    public static String[] parseLine(String s) throws IOException {
        return parser.parseLine(s);
    }

    /**
     * Parse line with a separator
     * @param s
     * @param separator
     * @return
     * @throws IOException
     */
    public static String[] parseLine(String s, char separator) throws IOException {
        CSVParser csvParser = new CSVParser(separator);
        return csvParser.parseLine(s);
    }
}

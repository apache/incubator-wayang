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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CsvRowConverter {

    private static final Logger logger = LogManager.getLogger(CsvRowConverter.class);

    private static CSVParser parser;

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

    public static BigDecimal parseDecimal(int precision, int scale, String string) {

        //System.out.println( ">>> " + string );
        BigDecimal result = null;

        try {

            result = new BigDecimal(string);
            //System.out.println( result );

            // If the parsed value has more fractional digits than the specified scale, round ties away from 0.
            if (result.scale() > scale) {
                logger.info(
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

        }
        catch (Exception ex) {
            ex.printStackTrace();
            throw new IllegalArgumentException(String
                    .format(Locale.ROOT, "BigDecimal %s can't be parsed.", string));
        }

        return result;

    }

    /**
     * Parse line with default separator.
     *
     * @param s - a line of data from a CSV file.
     * @return - array of strings, representing the field's data.
     *
     * @throws IOException
     */
    public static String[] parseLine(String s) throws IOException {
        return parser.parseLine(s);
    }



    /**
     * Parse line with a separator.
     *
     * If separator is '\0' (the null character), than we identify the separator.
     *
     * In case of a "null character" as separator we create a new CSVParser with the determined separator character.
     *
     * @param s - a line of data from a CSV file.
     * @param separator - a character used for splitting the line into fields using a CSVParser.
     * @return - array of strings, representing the field's data.
     *
     * @throws IOException
     */
    public static String[] parseLine(String s, char separator) throws IOException {

        if ( separator == '\0'  ) {

                separator = CSVDelimiterIdentifier.identifyDelimiter(s);
                CsvRowConverter.parser = new CSVParser(separator);

        }

        return parser.parseLine(s);

    }


}

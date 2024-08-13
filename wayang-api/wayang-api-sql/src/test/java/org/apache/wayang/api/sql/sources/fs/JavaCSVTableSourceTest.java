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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.DataSetType;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class JavaCSVTableSourceTest {

    private static final Logger logger = LogManager.getLogger(JavaCSVTableSourceTest.class);

    @Test
    public void testJavaCSVTableSource_with_autodelimiter() {

        logger.info( "*** JavaCSVTableSourceTest ***" );

        final String testFileName = "/banking-tx-small.csv";
        final URL inputUrl = this.getClass().getResource(testFileName);

        logger.info( "# testFileName : " + testFileName  );
        logger.info( "* input URL    : " + inputUrl  );

        String sourcePath = inputUrl.toString();

        RelDataType rowType = null;
        DataSetType dsType = DataSetType.createDefault(Record.class);
        List<RelDataType> fieldTypes = new ArrayList<>();
        //for (RelDataTypeField field : rowType.getFieldList()) {
        //    fieldTypes.add(field.getType());
        //}

        //'\0'
        JavaCSVTableSource s1 = new JavaCSVTableSource(sourcePath, dsType, fieldTypes, ','  );

        // we are able to instanciate the JavaCSVTableSource - not yet very useful, but a start.
        Assert.assertNotNull( s1 );

    }

}
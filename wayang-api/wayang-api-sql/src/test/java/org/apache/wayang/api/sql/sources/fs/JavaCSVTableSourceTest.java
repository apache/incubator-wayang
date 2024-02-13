package org.apache.wayang.api.sql.sources.fs;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.execution.JavaExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

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
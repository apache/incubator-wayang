package org.apache.wayang.api.sql.sources.fs;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CSVDelimiterIdentifierTest {
    @Test
    String getDelimiter() throws Exception {

        ModelParser modelParser;
        try {
            modelParser = new ModelParser();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String separator = modelParser.getSeparator();

        return separator;
    }

    @Test
    void identifyDelimiter() {

        System.out.println(">>> test delimiter identifier (CSVDelimiterIdentifier): ");
        String[] lines = {
                "1,2,3,4,5",
                "2;3;4;5;6",
                "1 2 3.4 4.5 5",
                "1\t2.3\t4\t7",
                "1|2.3|4|7"
        };

        char[] delimiter = {',',';',' ','\t','|'};

        int i = 0;
        for( String l : lines ) {
            char del = CSVDelimiterIdentifier.identifyDelimiter(l);
            System.out.println( " {" + del + "} <= " + l  );
            Assert.equals( del , delimiter[i] );
            i++;
        }

    }
}
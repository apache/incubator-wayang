package org.apache.wayang.plugin.hackit.core.tuple.header;

import org.assertj.core.api.MapAssert;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HeaderBuilderTest {

    @Test
    void generateHeader() {
        HeaderBuilder h = new HeaderBuilder();
        Header<Object> hh = h.generateHeader();
    }

    @Test
    void getConfiguration() {
        HeaderBuilder h = new HeaderBuilder();
        for (Map.Entry<String, String> l : h.getConfiguration().entrySet()) {
            Assert.assertTrue(l instanceof Map.Entry);
        }
    }

    @Test
    void setConfiguration() {
        HeaderBuilder h = new HeaderBuilder();
        h.setConfiguration("test", "test");
    }

}
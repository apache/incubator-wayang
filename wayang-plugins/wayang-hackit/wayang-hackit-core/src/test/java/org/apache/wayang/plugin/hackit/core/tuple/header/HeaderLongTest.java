package org.apache.wayang.plugin.hackit.core.tuple.header;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HeaderLongTest {

    @Test
    void createChild() {
        HeaderLong l = new HeaderLong(1l);
        HeaderLong ll = l.createChild();
        Assert.assertTrue(ll.getId() instanceof Long);
    }

    @Test
    void generateID() {
        HeaderLong l = new HeaderLong(1l);
        Long ll = l.generateID();
        Assert.assertTrue(ll instanceof Long);

    }
}
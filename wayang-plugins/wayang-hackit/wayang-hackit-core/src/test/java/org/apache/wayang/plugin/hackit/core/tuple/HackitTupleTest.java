package org.apache.wayang.plugin.hackit.core.tuple;

import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tags.LogTag;
import org.apache.wayang.plugin.hackit.core.tags.PauseTag;
import org.apache.wayang.plugin.hackit.core.tags.SkipTag;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;
import org.apache.wayang.plugin.hackit.core.tuple.header.HeaderLong;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class HackitTupleTest {

    @Test
    void getHeader() {
        String value = "test";
        HackitTuple<Long, String> ht = new HackitTuple<Long, String>(value);
        Header<Long> header = ht.getHeader();
        Assert.assertTrue(header instanceof Header);
        Assert.assertTrue(header.getId() instanceof Long);
    }

    @Test
    void getKey() {
        String value = "test";
        HackitTuple<Long, String> ht = new HackitTuple<Long, String>(value);
        Long k = ht.getKey();
        Assert.assertTrue(k instanceof Long);
    }

    @Test
    void getValue() {
        String value = "test";
        HackitTuple<Long, String> ht = new HackitTuple<Long, String>(value);
        String valueReturned = ht.getValue();
        Assert.assertEquals(value, valueReturned);
    }

    @Test
    void addTag() {
        String value = "test";
        HackitTuple<Long, String> ht = new HackitTuple<Long, String>(value);
        HackitTag logTag = new LogTag();
        ht.addTag(logTag);
        for (Iterator<HackitTag> it = ht.getTags(); it.hasNext(); ) {
            HackitTag t = it.next();
            Assert.assertTrue(logTag.equals(t));
        }

    }

    @Test
    void testAddTag() {
        String value = "test";
        HackitTuple<Long, String> ht = new HackitTuple<Long, String>(value);
        HackitTag tag1 = new LogTag();
        HackitTag tag2 = new PauseTag();
        HackitTag tag3 = new SkipTag();
        Set<HackitTag> tags = new HashSet<>();
        tags.add(tag1); tags.add(tag3); tags.add(tag2);
        ht.addTag(tags);
        for (Iterator<HackitTag> it = ht.getTags(); it.hasNext(); ) {
            HackitTag t = it.next();
            Assert.assertTrue(tags.contains(t));
        }
    }

    @Test
    void getTags() {
        String value = "test";
        HackitTuple<Long, String> ht = new HackitTuple<Long, String>(value);
        HackitTag logTag = new LogTag();
        ht.addTag(logTag);
        for (Iterator<HackitTag> it = ht.getTags(); it.hasNext(); ) {
            HackitTag t = it.next();
            Assert.assertTrue(logTag.equals(t));
        }
    }
}
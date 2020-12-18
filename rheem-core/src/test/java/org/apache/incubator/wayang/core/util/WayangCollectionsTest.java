package org.apache.incubator.wayang.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test suite for {@link WayangCollections}.
 */
public class WayangCollectionsTest {

    @Test
    public void testCreatePowerList() {
        final List<Integer> list = WayangArrays.asList(0, 1, 2, 3, 4);
        final Collection<List<Integer>> powerList = WayangCollections.createPowerList(list, 3);
        Assert.assertEquals(1 + 5 + 10 + 10, powerList.size());
        List<List<Integer>> expectedPowerSetMembers = Arrays.asList(
                WayangArrays.asList(),
                WayangArrays.asList(0), WayangArrays.asList(1), WayangArrays.asList(2), WayangArrays.asList(3), WayangArrays.asList(4),
                WayangArrays.asList(0, 1), WayangArrays.asList(0, 2), WayangArrays.asList(0, 3), WayangArrays.asList(0, 4), WayangArrays.asList(1, 2),
                WayangArrays.asList(1, 3), WayangArrays.asList(1, 4), WayangArrays.asList(2, 3), WayangArrays.asList(2, 4), WayangArrays.asList(3, 4),
                WayangArrays.asList(0, 1, 2), WayangArrays.asList(0, 1, 3), WayangArrays.asList(0, 1, 4), WayangArrays.asList(0, 2, 3), WayangArrays.asList(0, 2, 4),
                WayangArrays.asList(0, 3, 4), WayangArrays.asList(1, 2, 3), WayangArrays.asList(1, 2, 4), WayangArrays.asList(1, 3, 4), WayangArrays.asList(2, 3, 4)
        );
        for (List<Integer> expectedPowerSetMember : expectedPowerSetMembers) {
            Assert.assertTrue(String.format("%s is not contained in %s.", expectedPowerSetMember, powerList), powerList.contains(expectedPowerSetMember));
        }
    }

}

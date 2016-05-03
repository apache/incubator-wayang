package org.qcri.rheem.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Test suite for {@link RheemCollections}.
 */
public class RheemCollectionsTest {

    @Test
    public void testCreatePowerList() {
        final List<Integer> list = RheemArrays.asList(0, 1, 2, 3, 4);
        final Collection<List<Integer>> powerList = RheemCollections.createPowerList(list, 3);
        Assert.assertEquals(1 + 5 + 10 + 10, powerList.size());
        List<List<Integer>> expectedPowerSetMembers = Arrays.asList(
                RheemArrays.asList(),
                RheemArrays.asList(0), RheemArrays.asList(1), RheemArrays.asList(2), RheemArrays.asList(3), RheemArrays.asList(4),
                RheemArrays.asList(0, 1), RheemArrays.asList(0, 2), RheemArrays.asList(0, 3), RheemArrays.asList(0, 4), RheemArrays.asList(1, 2),
                RheemArrays.asList(1, 3), RheemArrays.asList(1, 4), RheemArrays.asList(2, 3), RheemArrays.asList(2, 4), RheemArrays.asList(3, 4),
                RheemArrays.asList(0, 1, 2), RheemArrays.asList(0, 1, 3), RheemArrays.asList(0, 1, 4), RheemArrays.asList(0, 2, 3), RheemArrays.asList(0, 2, 4),
                RheemArrays.asList(0, 3, 4), RheemArrays.asList(1, 2, 3), RheemArrays.asList(1, 2, 4), RheemArrays.asList(1, 3, 4), RheemArrays.asList(2, 3, 4)
        );
        for (List<Integer> expectedPowerSetMember : expectedPowerSetMembers) {
            Assert.assertTrue(String.format("%s is not contained in %s.", expectedPowerSetMember, powerList), powerList.contains(expectedPowerSetMember));
        }
    }

}
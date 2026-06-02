package org.apache.wayang.ml.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.wayang.ml.encoding.TreeNode;
import org.junit.jupiter.api.Test;

public class TreeNodeTest {
    @Test
    public void testEncodingFromString() throws IOException, URISyntaxException {
        String encoded = "((0,1,2,3),((4,5,6,7), ((8,9,10,11),((12,13,14,15),((16,17,18,19),((20,21,22,23),((24,25,26,27),),((28,29,30,31),)),((32,33,34,35),)),((36,37,38,39),)),((40,41,42,43),)),((44,45,46,47),)),((48,49,50,51),))";
        encoded = encoded.replaceAll("\\s+", "");
        final TreeNode decoded = TreeNode.fromString(encoded);

        assertEquals(encoded, decoded.toStringEncoding());
    }
}

package org.apache.wayang.plugin.hackit.core.tagger.wrapper;

import org.apache.wayang.plugin.hackit.core.tagger.wrapper.template.FunctionTemplateSystem;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class PredicateWrapperHackitTest {

    @Test
    public void testPredicateWrapper(){

        FunctionTemplateSystem<String, Boolean> func = new FunctionTemplateSystem<String, Boolean>() {
            @Override
            public Boolean execute(String input) {
                return input.length() > 6;
            }
        };

        PredicateWrapperHackit<String, String> wrap = new PredicateWrapperHackit<String, String>(func);

        String value = "test";
        HackitTuple<String, String> tuple = new HackitTuple(value);
        Boolean answer = wrap.execute(tuple);
        Assert.assertTrue(answer instanceof Boolean);
    }

}
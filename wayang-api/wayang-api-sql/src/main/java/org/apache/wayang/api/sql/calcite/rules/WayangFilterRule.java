package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;

public class WayangFilterRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalFilter.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangFilterRule")
            .withRuleFactory(WayangFilterRule::new);


    protected WayangFilterRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalFilter filter = (LogicalFilter) rel;
        return new WayangFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(WayangConvention.INSTANCE),
                convert(filter.getInput(), filter.getInput().getTraitSet().
                        replace(WayangConvention.INSTANCE)),
                filter.getCondition());
    }

}

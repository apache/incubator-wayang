package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WayangAggregateRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangAggregateRule")
            .withRuleFactory(WayangAggregateRule::new);

    protected WayangAggregateRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode relNode) {
        LogicalAggregate aggregate = (LogicalAggregate) relNode;
        RelNode input = convert(aggregate.getInput(), aggregate.getInput().getTraitSet().replace(WayangConvention.INSTANCE));

        return new WayangAggregate(
                aggregate.getCluster(),
                aggregate.getTraitSet().replace(WayangConvention.INSTANCE),
                aggregate.getHints(),
                input,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList()
        );
    }
}

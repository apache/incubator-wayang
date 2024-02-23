package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class WayangJoinRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    WayangConvention.INSTANCE, "WayangJoinRule")
            .withRuleFactory(WayangJoinRule::new);

    protected WayangJoinRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode relNode) {
        LogicalJoin join = (LogicalJoin) relNode;
        List<RelNode> newInputs = new ArrayList<>();
        for(RelNode input : join.getInputs()) {
            if(!(input.getConvention() instanceof WayangConvention)) {
                input = convert(input, input.getTraitSet().replace(WayangConvention.INSTANCE));
            }
            newInputs.add(input);
        }

        return new WayangJoin(
                join.getCluster(),
                join.getTraitSet().replace(WayangConvention.INSTANCE),
                newInputs.get(0),
                newInputs.get(1),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType()
        );
    }
}

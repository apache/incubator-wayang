package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangSort;

/**
 * Rule that converts {@link LogicalSort} to Wayang convention
 * {@link WayangSort}
 */
public class WayangSortRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalSort.class, Convention.NONE, WayangConvention.INSTANCE,
                    "WayangSortRule")
            .withRuleFactory(WayangSortRule::new);

    protected WayangSortRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode rel) {
        final LogicalSort sort = (LogicalSort) rel;

        final RelNode newInput = convert(
                sort.getInput(),
                sort.getInput()
                        .getTraitSet()
                        .replace(WayangConvention.INSTANCE));

        return new WayangSort(sort.getCluster(),
                sort.getTraitSet().replace(WayangConvention.INSTANCE),
                sort.getHints(),
                newInput,
                sort.collation,
                sort.fetch,
                sort.offset);
    }
}

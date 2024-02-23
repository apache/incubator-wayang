package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangTableScan;
import org.checkerframework.checker.nullness.qual.Nullable;

public class WayangTableScanRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalTableScan.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangTableScanRule")
            .withRuleFactory(WayangTableScanRule::new);

    public static final Config ENUMERABLE_CONFIG = Config.INSTANCE
            .withConversion(TableScan.class,
                    EnumerableConvention.INSTANCE, WayangConvention.INSTANCE,
                    "WayangTableScanRule1")
            .withRuleFactory(WayangTableScanRule::new);

    protected WayangTableScanRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode relNode) {

        TableScan scan = (TableScan) relNode;
        final RelOptTable relOptTable = scan.getTable();

        /**
         * This is quick hack to prevent volcano from merging projects on to TableScans
         * TODO: a cleaner way to handle this
         */
        if(relOptTable.getRowType() == scan.getRowType()) {
            return WayangTableScan.create(scan.getCluster(), relOptTable);
        }
        return null;
    }
}

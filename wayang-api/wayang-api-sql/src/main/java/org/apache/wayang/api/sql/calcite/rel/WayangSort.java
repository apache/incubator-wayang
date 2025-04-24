package org.apache.wayang.api.sql.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class WayangSort extends Sort implements WayangRel {
    public WayangSort(final RelOptCluster cluster,
            final RelTraitSet traits,
            final List<RelHint> hints,
            final RelNode child,
            final RelCollation collation,
            @Nullable final RexNode offset,
            @Nullable final RexNode fetch) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof WayangConvention;
    }

    @Override
    public Sort copy(final RelTraitSet traitSet, final RelNode newInput, final RelCollation newCollation, @Nullable final RexNode offset,
            @Nullable final RexNode fetch) {
        return new WayangSort(getCluster(), traitSet, getHints(), newInput, newCollation, offset, fetch);
    }

    @Override
    public String toString() {
        return "WayangSort";
    }
}

package org.apache.wayang.api.sql.calcite.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangProject;

public class WayangProjectRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalProject.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangProjectRule")
            .withRuleFactory(WayangProjectRule::new);


    protected WayangProjectRule(Config config) {
        super(config);
    }

    public RelNode convert(RelNode rel) {
        final LogicalProject project = (LogicalProject) rel;
        return new WayangProject(
                project.getCluster(),
                project.getTraitSet().replace(WayangConvention.INSTANCE),
                convert(project.getInput(), project.getInput().getTraitSet()
                        .replace(WayangConvention.INSTANCE)),
                project.getProjects(),
                project.getRowType());
    }
}

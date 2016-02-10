package org.qcri.rheem.core.api;

import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Configuration configuration = Configuration.createDefaultConfiguration(this);

    /**
     * Register a platform that Rheem will then use for execution.
     *
     * @param platform the {@link Platform} to register
     */
    public void register(Platform platform) {
        this.configuration.getPlatformProvider().addToWhitelist(platform);
    }

    /**
     * Execute a plan.
     *
     * @param physicalPlan the plan to execute
     */
    public void execute(PhysicalPlan physicalPlan) {
        this.createJob(physicalPlan).execute();
    }

    /**
     * Create a new {@link Job} that should execute the given {@link PhysicalPlan} eventually.
     */
    public Job createJob(PhysicalPlan physicalPlan) {
        return new Job(this, physicalPlan);
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}

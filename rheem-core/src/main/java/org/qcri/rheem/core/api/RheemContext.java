package org.qcri.rheem.core.api;

import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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
     * @param rheemPlan the plan to execute
     */
    public void execute(RheemPlan rheemPlan) {
        this.createJob(rheemPlan).execute();
    }

    /**
     * Create a new {@link Job} that should execute the given {@link RheemPlan} eventually.
     */
    public Job createJob(RheemPlan rheemPlan) {
        return new Job(this, rheemPlan);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}

package org.qcri.rheem.postgres.execution;

import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.platform.ExecutionProfile;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.postgres.PostgresPlatform;

/**
 * Created by yidris on 3/23/16.
 */
public class PostgresExecutor implements Executor {

    private PostgresPlatform platform;

    public PostgresExecutor(PostgresPlatform platform) {
        this.platform = platform;
    }

    @Override
    public ExecutionProfile execute(ExecutionStage stage) {
        return null;
    }

    @Override
    public void dispose() {

    }

    @Override
    public Platform getPlatform() {
        return platform;
    }
}

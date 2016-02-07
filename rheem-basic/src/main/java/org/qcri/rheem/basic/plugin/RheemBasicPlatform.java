package org.qcri.rheem.basic.plugin;

import org.qcri.rheem.basic.mapping.GlobalReduceMapping;
import org.qcri.rheem.basic.mapping.MaterializedGroupByMapping;
import org.qcri.rheem.basic.mapping.ReduceByMapping;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Activator for the basic Rheem package.
 */
@SuppressWarnings("unused") // It's loaded via reflection.
public class RheemBasicPlatform extends Platform {

    private final Collection<Mapping> mappings = new LinkedList<>();

    private static RheemBasicPlatform instance = null;

    public static RheemBasicPlatform getInstance() {
        if (instance == null) {
            instance = new RheemBasicPlatform();
        }
        return instance;
    }

    public RheemBasicPlatform() {
        super("Rheem Basic");
        initMappings();
    }

    private void initMappings() {
        this.mappings.add(new ReduceByMapping());
        this.mappings.add(new MaterializedGroupByMapping());
        this.mappings.add(new GlobalReduceMapping());
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        throw new RheemException("Platform is not executable.");
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public boolean isExecutable() {
        return false;
    }
}

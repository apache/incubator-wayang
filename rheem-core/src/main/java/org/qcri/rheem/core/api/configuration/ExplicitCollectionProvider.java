package org.qcri.rheem.core.api.configuration;

import org.qcri.rheem.core.api.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
 */
public class ExplicitCollectionProvider<Value> extends CollectionProvider<Value> {

    private static final Logger logger = LoggerFactory.getLogger(ExplicitCollectionProvider.class);

    private Set<Value> whitelist = new LinkedHashSet<>();

    private Set<Value> blacklist = new HashSet<>();

    public ExplicitCollectionProvider(Configuration configuration) {
        super(configuration);
    }

    public ExplicitCollectionProvider(Configuration configuration, CollectionProvider<Value> parent) {
        super(configuration, parent);
    }

    public boolean addToWhitelist(Value value) {
        if (this.blacklist.remove(value)) {
            logger.warn("{} was in the blacklist, moved it to the whitelist.", value);
        }
        return this.whitelist.add(value);
    }

    public void addAllToWhitelist(Collection<Value> values) {
        values.forEach(this::addToWhitelist);
    }

    public boolean addToBlacklist(Value value) {
        if (this.whitelist.remove(value)) {
            logger.warn("{} was in the whitelist, moved it to the blacklist.", value);
        }
        return this.blacklist.add(value);
    }

    public void addAllToBlacklist(Collection<Value> values) {
        values.forEach(this::addToBlacklist);
    }

    @Override
    public Collection<Value> provideAll(Configuration configuration) {
        Set<Value> containedValues = new LinkedHashSet<>();
        if (this.parent != null) {
            containedValues.addAll(this.parent.provideAll(configuration));
        }
        containedValues.addAll(this.whitelist);
        containedValues.removeAll(this.blacklist);
        return containedValues;
    }

}

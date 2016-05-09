package org.qcri.rheem.core.api.configuration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
 */
public class ExplicitCollectionProvider<Value> extends CollectionProvider<Value> {

    private Set<Value> whitelist = new HashSet<>();

    private Set<Value> blacklist = new HashSet<>();

    public ExplicitCollectionProvider(Configuration configuration) {
        super(configuration);
    }

    public ExplicitCollectionProvider(Configuration configuration, CollectionProvider<Value> parent) {
        super(configuration, parent);
    }

    public void addToWhitelist(Value value) {
        Validate.isTrue(!this.blacklist.contains(value), "%s is already in the blacklist.", value);
        this.whitelist.add(value);
    }

    public void addAllToWhitelist(Collection<Value> values) {
        Validate.isTrue(values.stream().noneMatch(this.blacklist::contains), "Some given value is already in the blacklist.");
        this.whitelist.addAll(values);
    }

    public void addToBlacklist(Value value) {
        Validate.isTrue(!this.whitelist.contains(value), "%s is already in the whitelist.", value);
        this.blacklist.add(value);
    }

    public void addAllToBlacklist(Collection<Value> values) {
        Validate.isTrue(values.stream().noneMatch(this.whitelist::contains), "Some given value is already in the whitelist.");
        this.blacklist.addAll(values);
    }

    @Override
    public Collection<Value> provideAll(Configuration configuration) {
        Set<Value> containedValues = new HashSet<>();
        if (this.parent != null) {
            containedValues.addAll(this.parent.provideAll(configuration));
        }
        containedValues.addAll(this.whitelist);
        containedValues.removeAll(this.blacklist);
        return containedValues;
    }

}

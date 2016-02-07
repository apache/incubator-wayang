package org.qcri.rheem.core.api.configuration;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Provides a {@link Collection} of objects.
 */
public class CollectionProvider<Value> implements Iterable<Value> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected CollectionProvider<Value> parent;

    private Set<Value> whitelist = new HashSet<>();

    private Set<Value> blacklist = new HashSet<>();

    private String warningSlf4j;

    public CollectionProvider() {
        this(null);
    }

    public CollectionProvider(CollectionProvider<Value> parent) {
        this.parent = parent;
    }

    public void setParent(CollectionProvider<Value> parent) {
        this.parent = parent;
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

    public Collection<Value> provideAll() {
        Set<Value> containedValues = new HashSet<>();
        Set<Value> excludedValues = new HashSet<>();
        return provideAll(containedValues, excludedValues);
    }

    private Collection<Value> provideAll(Set<Value> containedValues, Set<Value> excludedValues) {
        this.whitelist.stream().filter(value -> !excludedValues.contains(value)).forEach(containedValues::add);
        excludedValues.containsAll(this.blacklist);
        return this.parent == null ? containedValues : this.parent.provideAll(containedValues, excludedValues);
    }

    @Override
    public Iterator<Value> iterator() {
        return this.provideAll().iterator();
    }
}

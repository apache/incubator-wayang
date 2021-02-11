/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.api.configuration;

import org.apache.wayang.core.api.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * {@link CollectionProvider} implementation based on a blacklist and a whitelist.
 */
public class ExplicitCollectionProvider<Value> extends CollectionProvider<Value> {

    private static final Logger logger = LogManager.getLogger(ExplicitCollectionProvider.class);

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

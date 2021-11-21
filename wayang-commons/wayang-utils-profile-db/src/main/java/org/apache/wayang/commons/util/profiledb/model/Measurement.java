/*
 * Copyright 2016 Sebastian Kruse, Licensed under the Apache License, Version 2.0
 * https://github.com/sekruse/profiledb-java.git
 */

package org.apache.wayang.commons.util.profiledb.model;

import java.util.Objects;

/**
 *
 * Measurement captures the value of a metric at a specific time
 */
public abstract class Measurement {

    private String id;

    /**
     * Returns implementation Class of this Measurement
     */
    public static String getTypeName(Class<? extends Measurement> measurementClass) {
        return measurementClass.getDeclaredAnnotation(Type.class).value();
    }

    /**
     * Deserialization constructor.
     */
    protected Measurement() {
    }

    public Measurement(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return getTypeName(this.getClass());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measurement that = (Measurement) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

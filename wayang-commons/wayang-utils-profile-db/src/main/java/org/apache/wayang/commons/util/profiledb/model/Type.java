/*
 * Copyright 2016 Sebastian Kruse, Licensed under the Apache License, Version 2.0
 * https://github.com/sekruse/profiledb-java.git
 */

package org.apache.wayang.commons.util.profiledb.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Type {

    String value();
}

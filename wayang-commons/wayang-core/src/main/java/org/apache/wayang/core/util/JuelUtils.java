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

package org.apache.wayang.core.util;

import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.TreeValueExpression;
import de.odysseus.el.util.SimpleContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationUtils;
import org.apache.logging.log4j.LogManager;

import javax.el.ValueExpression;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities to deal with JUEL expressions.
 */
public class JuelUtils {

    public static class JuelFunction<T> {

        private final Map<String, Argument> arguments = new HashMap<>();

        private final ExpressionFactoryImpl expressionFactory;

        private final SimpleContext context;

        private final ValueExpression expression;


        public JuelFunction(String juelExpression, Class<T> resultClass, Map<String, Class<?>> arguments) {
            // Initialize the JUEL conext.
            this.expressionFactory = new de.odysseus.el.ExpressionFactoryImpl();
            this.initializeContext(this.context = new SimpleContext());

            // Index the arguments.
            Class<?>[] argumentTypeClasses = new Class[arguments.size()];
            int argIndex = 0;
            for (Map.Entry<String, Class<?>> argumentEntry : arguments.entrySet()) {
                final String argName = argumentEntry.getKey();
                final Class<?> argTypeClass = argumentEntry.getValue();
                final TreeValueExpression argExpression =
                        this.expressionFactory.createValueExpression(this.context, String.format("${%s}", argName), argTypeClass);
                Argument argument = new Argument(argIndex++, argTypeClass, argExpression);
                argumentTypeClasses[argument.index] = argument.typeClass;
                this.arguments.put(argName, argument);
            }

            // Create the JUEL method.
            this.expression = expressionFactory.createValueExpression(this.context, juelExpression, resultClass);
//            this.expression = expressionFactory.createMethodExpression(this.context, juelExpression, resultClass, argumentTypeClasses);
        }

        private void initializeContext(SimpleContext ctx) {
            try {
                ctx.setFunction("math", "sqrt", Math.class.getMethod("sqrt", double.class));
                ctx.setFunction("wayang", "logGrowth", OptimizationUtils.class.getMethod(
                        "logisticGrowth", double.class, double.class, double.class, double.class)
                );
            } catch (NoSuchMethodException e) {
                throw new WayangException("Could not initialize JUEL context.", e);
            }
        }

        @SuppressWarnings("unchecked")
        public T apply(Map<String, Object> values) {
            return this.apply(values, false);
        }

        @SuppressWarnings("unchecked")
        public T apply(Map<String, Object> values, boolean isExpectTooManyArguments) {
            values.forEach((key, value) -> {
                final Argument argument = this.arguments.get(key);
                if (argument == null) {
                    if (isExpectTooManyArguments) {
                        LogManager.getLogger(this.getClass()).debug("Unknown field \"{}\" (available: {}).", key, this.arguments.keySet());
                    } else {
                        LogManager.getLogger(this.getClass()).warn("Unknown field \"{}\" (available: {}).", key, this.arguments.keySet());
                    }
                } else {
                    argument.expression.setValue(this.context, value);
                }
            });
            return (T) this.expression.getValue(this.context);
        }
    }

    /**
     * This class describes arguments of {@link JuelFunction}s.
     */
    private static final class Argument {

        private final int index;

        private final Class<?> typeClass;

        private final ValueExpression expression;

        private Argument(int index, Class<?> typeClass, ValueExpression expression) {
            this.index = index;
            this.typeClass = typeClass;
            this.expression = expression;
        }
    }

}

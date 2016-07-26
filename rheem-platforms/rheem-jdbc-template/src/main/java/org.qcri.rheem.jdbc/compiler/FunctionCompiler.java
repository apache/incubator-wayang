package org.qcri.rheem.jdbc.compiler;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.PredicateDescriptor;


import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.function.Predicate;

/**
 * Created by yidris on 4/5/16.
 */
public class FunctionCompiler {

    @Retention(RetentionPolicy.RUNTIME)
    public @interface SQL {
        String value();
    }

    /**
     * Compile a predicate to a SQL where clause.
     * For now we just assume the test method of the
     * predicate is annotated with the where clause using the @SQL
     * annotation. However, we should eventually use a java analysis tool
     * to convert the predicate to a SQL string.
     * See <a href="https://jitpack.io/#ajermakovics/lambda2sql/v0.1">lambda2sql</a>
     * @param descriptor describes the predicate
     * @return a compiled SQL where clause.
     */
    public String compile(PredicateDescriptor descriptor) {

        PredicateDescriptor.SerializablePredicate predicate = descriptor.getJavaImplementation();
        try {
            Method testMethod = predicate.getClass().getMethod("test", descriptor.getInputType().getTypeClass());
            SQL whereClause = (SQL)testMethod.getAnnotation(SQL.class);
            return whereClause.value();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RheemException("Could not compile predicate to SQL: ", e);
        }
    }


//    /**
//     * Compile a predicate to a JPA JPQL where clause.
//     *
//     * @param descriptor describes the predicate
//     * @return a JPA JPQL where clause.
//     */
//    public String compileToJPQL(PredicateDescriptor descriptor) {
//        return  "";
//    }


    /**
     * Compile to a jpa criteria builder
     */
}

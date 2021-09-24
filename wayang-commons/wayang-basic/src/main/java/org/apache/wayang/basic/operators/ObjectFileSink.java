package org.apache.wayang.basic.operators;

import java.util.Objects;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.DefaultLoadEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

/**
 * This {@link UnarySink} writes all incoming data quanta to a Object file.
 *
 * @param <T> type of the object to store
 */
public class ObjectFileSink<T> extends UnarySink<T> {

  protected final String textFileUrl;

  protected final Class<T> tClass;

  /**
   * Creates a new instance.
   *
   * @param targetPath  URL to file that should be written
   * @param type        {@link DataSetType} of the incoming data quanta
   */
  public ObjectFileSink(String targetPath, DataSetType<T> type) {
    super(type);
    this.textFileUrl = targetPath;
    this.tClass = type.getDataUnitType().getTypeClass();
  }

  /**
   * Creates a new instance.
   *
   * @param textFileUrl        URL to file that should be written
   * @param typeClass          {@link Class} of incoming data quanta
   */
  public ObjectFileSink(String textFileUrl, Class<T> typeClass) {
    super(DataSetType.createDefault(typeClass));
    this.textFileUrl = textFileUrl;
    this.tClass = typeClass;
  }

  /**
   * Creates a copied instance.
   *
   * @param that should be copied
   */
  public ObjectFileSink(ObjectFileSink<T> that) {
    super(that);
    this.textFileUrl = that.textFileUrl;
    this.tClass = that.tClass;
  }
}

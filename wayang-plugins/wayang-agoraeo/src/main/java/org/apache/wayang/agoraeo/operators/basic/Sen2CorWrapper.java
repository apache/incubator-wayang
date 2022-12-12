package org.apache.wayang.agoraeo.operators.basic;

import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.core.types.DataSetType;

public class Sen2CorWrapper
    extends UnaryToUnaryOperator<String, String>
{
    public String sen2cor;
    public String l2a_location;

    public Sen2CorWrapper(String sen2cor, String l2a_location){
        super(
                DataSetType.createDefault(BasicDataUnitType.createBasic(String.class)),
                DataSetType.createDefault(BasicDataUnitType.createBasic(String.class)),
                false
        );
        this.sen2cor = sen2cor;
        this.l2a_location = l2a_location;
    }

    public Sen2CorWrapper(UnaryToUnaryOperator<String, String> that) {
        super(that);
    }


}

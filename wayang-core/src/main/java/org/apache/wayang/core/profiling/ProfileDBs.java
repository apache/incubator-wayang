package org.apache.incubator.wayang.core.profiling;

import de.hpi.isg.profiledb.ProfileDB;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.incubator.wayang.core.plan.wayangplan.PlanMetrics;

/**
 * Utilities to work with {@link de.hpi.isg.profiledb.ProfileDB}s.
 */
public class ProfileDBs {

    /**
     * Create and customize a {@link ProfileDB}.
     *
     * @return the {@link ProfileDB}
     */
    public static ProfileDB createProfileDB() {
        final ProfileDB profileDB = new ProfileDB();
        customize(profileDB);
        return profileDB;
    }

    /**
     * Customize a {@link ProfileDB} for use with Wayang.
     *
     * @param profileDB the {@link ProfileDB}
     */
    public static void customize(ProfileDB profileDB) {
        profileDB
                .withGsonPreparation(
                        gsonBuilder -> gsonBuilder.registerTypeAdapter(Operator.class, new OperatorBase.GsonSerializer())
                )
                .registerMeasurementClass(CostMeasurement.class)
                .registerMeasurementClass(PlanMetrics.class);
    }

}

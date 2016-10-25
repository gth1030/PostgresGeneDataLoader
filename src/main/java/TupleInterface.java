/**
 * Created by kitae on 8/1/16.
 * Allows access to both feature and featureloc from functions while keeping good size container for both classes.
 */
public class TupleInterface {
    /**
     * Maps string key to actual value as if it is a map.
     * @param value key for the value
     * @param tupl tuple class that contains information
     * @return value that matches key from given tuple.
     */
    static String getProperValue(String value, TupleInterface tupl) {
        if (tupl.getClass().equals(FeatureTuple.class)) {
            if (value.equals("feature_id")) {
                return ((FeatureTuple) tupl).feature_id;
            } else if (value.equals("seqlen")) {
                return ((FeatureTuple) tupl).seqlen;
            } else if (value.equals("uniquename")) {
                return ((FeatureTuple) tupl).uniquename;
            } else if (value.equals("type_id")) {
                return ((FeatureTuple) tupl).type_id;
            } else if (value.equals("organism_id")) {
                return ((FeatureTuple) tupl).organism_id;
            } else if (value.equals("is_analysis")) {
                return ((FeatureTuple) tupl).is_analysis;
            }
        } else if (tupl.getClass().equals(FeatureLocTuple.class)) {
            if (value.equals("srcfeature_id")) {
                return ((FeatureLocTuple) tupl).srcfeature_id;
            } else if (value.equals("feature_id")) {
                return ((FeatureLocTuple) tupl).parent.feature_id;
            } else if (value.equals("locgroup")) {
                return ((FeatureLocTuple) tupl).locgroup;
            } else if (value.equals("rank")) {
                return ((FeatureLocTuple) tupl).rank;
            } else if (value.equals("fmin")) {
                return ((FeatureLocTuple) tupl).fmin;
            } else if (value.equals("fmax")) {
                return ((FeatureLocTuple) tupl).fmax;
            } else if (value.equals("phase")) {
                return ((FeatureLocTuple) tupl).phase;
            } else if (value.equals("strand")) {
                return ((FeatureLocTuple) tupl).strand;
            }
        }
        throw new IllegalArgumentException("Particular variable name called : " + value + " is unrecognized!");
    }
}

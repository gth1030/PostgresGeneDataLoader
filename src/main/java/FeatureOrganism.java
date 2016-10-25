/**
 * Created by kitae on 8/1/16.
 * functions as a key for some maps. Provides clean access to maps in bedfileConvertor.
 */
public class FeatureOrganism {
    FeatureOrganism(String feature, String organism) {
        feature_id = feature;
        organism_id = organism;
    }

    @Override
    public String toString() {
        return "feature_id: " + feature_id + " organism_id: " + organism_id;
    }


    public boolean equals(String featureID, String organism_id) {
        if (featureID.equals(feature_id) && organism_id.equals(organism_id))
            return true;
        return false;
    }

    String feature_id;
    String organism_id;
}

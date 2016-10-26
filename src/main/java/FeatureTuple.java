import java.util.Queue;

/**
 * Created by kitae on 5/13/16.
 */
public class FeatureTuple extends TupleInterface {

    protected FeatureTuple(String[] token, JsonTuple jtupl) {
        int index = Integer.parseInt(jtupl.index);
        int start = Integer.parseInt(jtupl.columns.get("START")) - index; // Correct start based on index
        int end = Integer.parseInt(jtupl.columns.get("END"));
        seqlen = Integer.toString(Integer.parseInt(token[end]) - Integer.parseInt(token[start]));
        type_id = BedFileConvertor.dbxrefToCvtermMap.get(jtupl.getTypeID(token));
        organism_id = BedFileConvertor.dbxrefToFeatureOrganismMap.get(token[jtupl.getSrcFeatureIndex()]).organism_id;
        uniquename = jtupl.generateName(type_id);
        if (jtupl.columns.containsKey("RAWSCORE")) {
            rawscore = jtupl.columns.get("RAWSCORE");
        } else if (jtupl.columns.containsKey("NORMSCORE")) {
            normscore = jtupl.columns.get("NORMSCORE");
        } else if (jtupl.columns.containsKey("SIGNIFICANCE")) {
            significance = jtupl.columns.get("SIGNIFICANCE");
        } else if (jtupl.columns.containsKey("IDENTITY")) {
            identity = jtupl.columns.get("IDENTITY");
        } else if (jtupl.columns.containsKey("PERCENTILE")) {
            percentile = jtupl.columns.get("PERCENTILE");
        } else if (jtupl.columns.containsKey("RANK")) {
            rank = jtupl.columns.get("RANK");
        } else if (jtupl.columns.containsKey("ERROR")) {
            error = jtupl.columns.get("ERROR");
        } else if (jtupl.columns.containsKey("ERROR2")) {
            error2 = jtupl.columns.get("ERROR2");
        }
        is_analysis = "true";
    }

    /** Constructor for condensed type jsonfile. */
    FeatureTuple(JsonTuple jtupl, String type_idI) {
        seqlen = "";
        uniquename = jtupl.generateName(type_idI);
        type_id = type_idI;
        is_analysis = "true";
        /* organism_id is assigned later.*/
        organism_id = null;
    }

    /**
     * For all features that are present in json file but not used in data file is removed by checking their
     * organism id.
     * @param featureQueue
     */
    public static void filterUnusedFeature_Condensed(Queue<FeatureTuple> featureQueue) {
        for (int i = 0; i < featureQueue.size(); i++) {
            FeatureTuple tuple = featureQueue.poll();
            if (tuple.organism_id == null) {
                continue;
            }
            featureQueue.add(tuple);
        }
    }

    /**
     * Maps column names for analysisfeature table to their values.
     * @param columnName Name of column for analysisfeature columns.
     * @return value of specific column name for analysisFeature
     */
    public String getProperFeatureAnalysisVal(String columnName) {
        if (columnName.equals("RAWSCORE")) {
            return rawscore;
        } else if (columnName.equals("NORMSCORE")) {
            return normscore;
        } else if (columnName.equals("SIGNIFICANCE")) {
            return significance;
        } else if (columnName.equals("IDENTITY")) {
            return identity;
        } else if (columnName.equals("PERCENTILE")) {
            return percentile;
        } else if (columnName.equals("RANK")) {
            return rank;
        } else if (columnName.equals("ERROR")) {
            return error;
        } else if (columnName.equals("ERROR2")) {
            return error2;
        }
        return null;
    }


    /* Columns for feature table */
    String feature_id;
    String seqlen;
    String uniquename;
    String type_id;
    String organism_id;
    String is_analysis;
    String rawscore;
    String normscore;
    String significance;
    String identity;
    String percentile;
    String rank;
    String error;
    String error2;

}

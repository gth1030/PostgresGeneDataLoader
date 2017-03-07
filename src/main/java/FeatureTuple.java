import java.util.Queue;

/**
 * Created by kitae on 5/13/16.
 */
public class FeatureTuple extends TupleInterface {

    protected FeatureTuple(String[] token, JsonTuple jTuple) {
        int index = Integer.parseInt(jTuple.index);
        int start = Integer.parseInt(jTuple.columns.get("START")) - index; // Correct start based on index
        int end = Integer.parseInt(jTuple.columns.get("END"));
        seqlen = Integer.toString(Integer.parseInt(token[end]) - Integer.parseInt(token[start]));
        type_id = BedFileConvertor.dbxrefToCvtermMap.get(jTuple.getTypeID(token));
        organism_id = BedFileConvertor.dbxrefToFeatureOrganismMap.get(token[jTuple.getSrcFeatureIndex()]).organism_id;
        uniquename = jTuple.generateName(type_id);
        if (jTuple.columns.containsKey("RAWSCORE")) {
            rawscore = jTuple.columns.get("RAWSCORE");
        }
        if (jTuple.columns.containsKey("NORMSCORE")) {
            normscore = jTuple.columns.get("NORMSCORE");
        }
        if (jTuple.columns.containsKey("SIGNIFICANCE")) {
            significance = jTuple.columns.get("SIGNIFICANCE");
        }
        if (jTuple.columns.containsKey("IDENTITY")) {
            identity = jTuple.columns.get("IDENTITY");
        }
        if (jTuple.columns.containsKey("PERCENTILE")) {
            percentile = jTuple.columns.get("PERCENTILE");
        }
        if (jTuple.columns.containsKey("RANK")) {
            rank = jTuple.columns.get("RANK");
        }
        if (jTuple.columns.containsKey("ERROR")) {
            error = jTuple.columns.get("ERROR");
        }
        if (jTuple.columns.containsKey("ERROR2")) {
            error2 = jTuple.columns.get("ERROR2");
        }
        if (jTuple.columns.containsKey("FEATURE_DBXREF")) {
            featureDbxrefFullAcc = jTuple.columns.get("FEATURE_DBXREF");
        }
        is_analysis = "true";
    }

    /** Constructor for condensed type jsonfile. */
    FeatureTuple(JsonTuple jTuple, String type_idI) {
        seqlen = "";
        uniquename = jTuple.generateName(type_idI);
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
        String value = "";
        if (columnName.equals("RAWSCORE")) {
            value = (rawscore != null) ? rawscore : value;
        } else if (columnName.equals("NORMSCORE")) {
            value = (normscore != null) ? normscore : value;
        } else if (columnName.equals("SIGNIFICANCE")) {
            value = (significance != null) ? significance : value;
        } else if (columnName.equals("IDENTITY")) {
            value = (identity != null) ? identity : value;
        } else if (columnName.equals("PERCENTILE")) {
            value = (percentile != null) ? percentile : value;
        } else if (columnName.equals("RANK")) {
            value = (rank != null) ? rank : value;
        } else if (columnName.equals("ERROR")) {
            value = (error != null) ? error : value;
        } else if (columnName.equals("ERROR2")) {
            value = (error2 != null) ? error2 : value;
        }
        return value;
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


    /** if dbxref is present, featuredbxref is added. The format is dbname:dbxrefAcession[:version] (version is optional) **/
    String featureDbxrefFullAcc;
    /** Mapped later when dbxref is created, and used to upload feature_dbxref.**/
    String dbxref_id;
    /** Mapped when dbxrefis created, and used to upload feature_dbxref **/
    String feature_dbxref_id;

}

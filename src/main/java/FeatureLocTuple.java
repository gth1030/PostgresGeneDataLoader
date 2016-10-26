import java.util.Map;

/**
 * Created by kitae on 5/13/16.
 */
public class FeatureLocTuple extends TupleInterface {


    FeatureLocTuple(String[] token, int rankC, JsonTuple jtupl, FeatureTuple feature) {
        srcfeature_id = BedFileConvertor.dbxrefToFeatureOrganismMap.get(token[jtupl.getSrcFeatureIndex()]).feature_id;
        rank = Integer.toString(rankC);
        fmin = Integer.toString(Integer.parseInt(token[Integer.parseInt(jtupl.columns.get("START"))]) - Integer.parseInt(jtupl.index));
        fmax = token[Integer.parseInt(jtupl.columns.get("END"))];
        if (jtupl.columns.containsKey("LOCGROUP")) {
            locgroup = token[Integer.parseInt(jtupl.columns.get("LOCGROUP"))];
        }
        if (jtupl.columns.containsKey("PHASE")) {
            phase = token[Integer.parseInt(jtupl.columns.get("PHASE"))];
        }
        if (jtupl.columns.containsKey("STRAND")) {
            strand = token[Integer.parseInt((jtupl.columns.get("STRAND")))];
            if (token[Integer.parseInt((jtupl.columns.get("STRAND")))].equals("+")) {
                strand = "1";
            } else if (token[Integer.parseInt((jtupl.columns.get("STRAND")))].equals("-")) {
                strand = "-1";
            }
        }
        parent = feature;
    }

    /**
     * Returns rank for each featureLoc data tuple. For each type, integer value is assigned and incremented by 1 for
     * every time new featureloc with same type is called.
     * @param rankCounter Map containing typeID as key, and current counter as value
     * @param jTupl json file object
     * @param dataTuple a row of data file.
     * @return rank value
     */
    static int getRank(Map<String, Integer> rankCounter, JsonTuple jTupl, String[] dataTuple) {
        int rank = 0;
        if (rankCounter.containsKey(jTupl.getTypeID(dataTuple))) {
            rank = rankCounter.get(jTupl.getTypeID(dataTuple));
            rankCounter.put(jTupl.getTypeID(dataTuple), rank + 1);
        }
        return rank;
    }

    /* Columns for featureLoc Table */
    String srcfeature_id;
    String locgroup = "0";
    String rank;
    String fmin;
    String fmax;
    String phase = "";
    String strand = "";
    /* Parent feature tuple for this featureloc tuple. */
    FeatureTuple parent;
}

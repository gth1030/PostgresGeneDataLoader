import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by kitae on 5/18/16.
 */
class JsonFileSanityCheck {


    /* Very simple function that creates string query for postgres execution. */
    private static String constructQueryForDbAccessionFeatureOrganismid(JsonTuple jtupl) {
        StringBuilder query = new StringBuilder("SELECT db.name AS db, dbxref.accession, dbxref.version, feature_id, organism_id " +
                "FROM feature JOIN feature_dbxref USING(feature_id) JOIN dbxref ON (feature_dbxref.dbxref_id = dbxref.dbxref_id) JOIN db " +
                "USING(db_id) JOIN (VALUES");
        query.append("(?,?,?)");
        for (int i = 1; i < jtupl.srcfeature.size(); i++) {
            query.append(",(?,?,?)");
        }
        query.append(") AS temp(db,accession,version) ON (db.name = temp.db AND dbxref.accession = " +
                "temp.accession AND dbxref.version = temp.version);");
        return query.toString();
    }

    /* checks if srcFeature in jsonfile is mapped in the database, and returns map that connects srcfeature and feature_id,
    and organism_id */
    static Map<String, FeatureOrganism> generateDbxrefToFeatureOrganismMap(JsonTuple jtupl, Connection connection)
            throws SQLException {
        String query1 = constructQueryForDbAccessionFeatureOrganismid(jtupl);
        PreparedStatement preparedState = connection.prepareStatement(query1);
        int i = 0;
        for (String srcft : jtupl.srcfeature.keySet()) {
            DbAccessionVersion DAV = BedFileConvertor.makeDbAccessionVersion(jtupl.srcfeature.get(srcft));
            DAV.assignStrToPeparedStatement(i, preparedState);
            i++;
        }
        ResultSet result = preparedState.executeQuery();
        HashMap<String, FeatureOrganism> dbxrefToFeatureOrganismMap = new HashMap<String, FeatureOrganism>();
        Set<String> corruptresult = new HashSet<String>();
        while(result.next()) {
            String key = getMatchingKey(jtupl.srcfeature, result.getString("db"),
                    result.getString("accession"), result.getString("version"));
            String featureID = result.getString("feature_id");
            String organismID = result.getString("organism_id");
            if (dbxrefToFeatureOrganismMap.containsKey(key) && (
                    !dbxrefToFeatureOrganismMap.get(key).equals(featureID, organismID))) {
                corruptresult.add(key + " - " + featureID + " - " + organismID);
            }
            dbxrefToFeatureOrganismMap.put(key, new FeatureOrganism(featureID, organismID));
        }
        if (corruptresult.size() > 0) {
            throw new IllegalArgumentException("More than one feature_id or organism_id are connected to one or more data entry.\n" +
                    "Erroneous data tuples: " + corruptresult.toString());
        }
        if (jtupl.srcfeature.size() != dbxrefToFeatureOrganismMap.size()) {
            Set<String> listOfMissingFeature = new HashSet<String>();
            for (String srcFeatureKey : jtupl.srcfeature.keySet()) {
                if (!dbxrefToFeatureOrganismMap.containsKey(srcFeatureKey)) {
                    listOfMissingFeature.add(srcFeatureKey);
                }
            }
            throw new IllegalArgumentException("Some of the source features are not mapped by database. \nThe features are:" + listOfMissingFeature);
        }
        return dbxrefToFeatureOrganismMap;
    }


    /**
     * Generates dbxref(dbname, accession, version) to cvterm_id mapping using cvterm talbe and cvterm_dbxref table.
     * Checks if cvprop properly maps to postgres
     * @param jtupl json file object with information
     * @param connection connection to postgres
     * @return dbxref to cvterm amp
     * @throws SQLException
     */
    static Map<String, String> generateDbxrefToCvtermMap(JsonTuple jtupl, Connection connection) throws SQLException {
        HashMap<String, String> dbrefTocvtermMap = new HashMap<String, String>();
        Set<String> corruptresult = new HashSet<String>();
        List<String> typeList = new ArrayList<String>(jtupl.type.keySet());
        if (jtupl.analysis != null && jtupl.analysis.cvprop != null) {
            for (String key : jtupl.analysis.cvprop.experimentMap.keySet()) {
                for (String expKey : jtupl.analysis.cvprop.experimentMap.get(key).tupleMap.keySet()) {
                    typeList.addAll(jtupl.analysis.cvprop.experimentMap.get(key).tupleMap.get(expKey));
                }
            }
        }
        int ind = 0;
        while (ind < typeList.size()){
            PreparedStatement preparedStatement = createPreparedStatement(ind, "cvterm", typeList, jtupl, connection);
            typeResultSetProcessor(jtupl, preparedStatement, dbrefTocvtermMap, corruptresult);
            preparedStatement = createPreparedStatement(ind, "cvterm_dbxref", typeList, jtupl, connection);
            typeResultSetProcessor(jtupl, preparedStatement, dbrefTocvtermMap, corruptresult);
            ind += 100;
        }
        if (corruptresult.size() > 0) {
            throw new IllegalArgumentException("Some cvterm_ids are connected to more than one db mapping.\n" +
                    "Erroneous data tuples: " + corruptresult.toString());
        }
        if (typeList.size() != dbrefTocvtermMap.size()) {
            Set<String> listOfMissingType = new HashSet<String>();
            for (String cvpropVal : typeList) {
                if (!dbrefTocvtermMap.containsKey(cvpropVal)) {
                    listOfMissingType.add(cvpropVal);
                }
            }
            if (listOfMissingType.size() > 0)
                throw new IllegalArgumentException("Some of the types or cvproperties are not mapped by database.  \n" +
                    "The features are: " + listOfMissingType);
        }
        return dbrefTocvtermMap;
    }

    private static PreparedStatement createPreparedStatement(int ind, String tableName, List<String> keyList, JsonTuple jtupl, Connection connection) throws SQLException {
        int roof = Math.min(keyList.size() - ind, 100);
        String query = typeCheckQueryBuilder(roof, tableName);
        PreparedStatement preparedState = connection.prepareStatement(query);
        for (int i = 0; i < roof; i++) {
            DbAccessionVersion DAV = BedFileConvertor.makeDbAccessionVersion(jtupl.type.get(keyList.get(ind + i)));
            DAV.assignStrToPeparedStatement(i, preparedState);
        }
        return preparedState;
    }

    private static String typeCheckQueryBuilder(int count, String cvtableName) {
        StringBuilder query = new StringBuilder("SELECT db.name AS db, dbxref.accession, dbxref.version, cvterm_id " +
                "FROM " + cvtableName + " JOIN dbxref USING(dbxref_id) JOIN db USING(db_id) JOIN (VALUES");
        query.append("(?,?,?)");
        for (int i = 1; i < count; i++) {
            query.append(",(?,?,?)");
        }
        query.append(") AS temp(db,accession,version) ON(db.name = temp.db AND dbxref.accession " +
                "= temp.accession AND dbxref.version = temp.version);");
        return query.toString();
    }

    private static void typeResultSetProcessor(JsonTuple jtupl, PreparedStatement preparedStatement,
                                               Map<String, String> dbxrefToCvtermMap,
                                               Set<String> corruptSet) throws SQLException {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String key = getMatchingKey(jtupl.type, resultSet.getString("db"), resultSet.getString("accession"),
                    resultSet.getString("version"));
            if (key == null) {
                if (jtupl.analysis != null && jtupl.analysis.cvprop != null) {
                    key = getMatchingKeyForCv(jtupl, resultSet.getString("db"), resultSet.getString("accession"),
                            resultSet.getString("version"));
                }
                if (key == null) {
                    throw new IllegalArgumentException("Program failed to generate key for cvTerm ID map for either type or cvProperty!");
                }
            }
            if (dbxrefToCvtermMap.containsKey(key) && !dbxrefToCvtermMap.get(key).equals(resultSet.getString("cvterm_id"))) {
                corruptSet.add(key + " - " + resultSet.getString("cvterm_id"));
            }
            dbxrefToCvtermMap.put(key, resultSet.getString("cvterm_id"));
        }
    }


    /**
     * Checks if cvterms are properly mapped in postgres
     * @param jtupl json file object with information
     * @param connection connection to postgres
     * @return cvname to cv_id map
     */
    static Map<DbCvname, String> generateCvnameToCvIDMap(JsonTuple jtupl, Connection connection) {
        StringBuilder query = new StringBuilder("SELECT cv.name as cv, cvterm.name as term, cvterm_id " +
                "FROM cvterm JOIN cv USING(cv_id) JOIN (VALUES");

        Map<String, String> propAndCvpropValMap = new HashMap<String, String>();
        if (jtupl.analysis.cvprop != null) {
            for (String key : jtupl.analysis.cvprop.experimentMap.keySet()) {
                JsonTuple.Analysis.Experiment experiment = jtupl.analysis.cvprop.experimentMap.get(key);
                for (String val : experiment.tupleMap.keySet()) {
                    propAndCvpropValMap.put(key, val);
                }
            }
        }
        if (jtupl.analysis.property != null) {
            for (String key : jtupl.analysis.property.experimentMap.keySet()) {
                JsonTuple.Analysis.Experiment experiment = jtupl.analysis.property.experimentMap.get(key);
                for (String val : experiment.tupleMap.keySet()) {
                    propAndCvpropValMap.put(key, val);
                }
            }
        }
        for (int i = 0; i < propAndCvpropValMap.size(); i++) {
            query.append("(?,?),");
        }
        query.setLength(query.length() - 1);
        query.append(") AS temp(cv, term) ON(cv.name = temp.cv AND cvterm.name = temp.term);");
        try {
            PreparedStatement preparedState = connection.prepareStatement(query.toString());
            int insertionCount = 1;
            for (Map.Entry<String, String> entry : propAndCvpropValMap.entrySet()) {
                preparedState.setString(insertionCount, entry.getKey());
                preparedState.setString(insertionCount + 1, entry.getValue());
                insertionCount += 2;
            }
            ResultSet result = preparedState.executeQuery();
            HashMap<DbCvname, String> cvTocvID = new HashMap<DbCvname, String>();
            Set<String> correctresult = new HashSet<String>();
            while (result.next()) {
                DbCvname key = new DbCvname(result.getString("cv"), result.getString("term"));
                if (cvTocvID.containsKey(key) && (
                        !cvTocvID.get(key).equals(result.getString("cvterm_id")))) {
                    correctresult.add(key.toString() + " - " + result.getString("cvterm_id"));
                }
                cvTocvID.put(key, result.getString("cvterm_id"));
            }
            if (correctresult.size() > 0) {
                throw new IllegalArgumentException("Some cvterm_ids are connected to more than one cv&cvname entry.\n" +
                        "Erroneous data tuple includes : " + correctresult.toString());
            }
            if (propAndCvpropValMap.size() != cvTocvID.size()) {
                System.err.println("Detected cvterms = " + propAndCvpropValMap.keySet() + ". Mapped cvterms = " + cvTocvID.keySet());
                throw new IllegalArgumentException("Some of the cvterms are not mapped by database.");
            }
            return cvTocvID;
        } catch (SQLException e) {
            throw new IllegalArgumentException("SQL exception during CVname mapping and sanity check! " + e);
        }
    }

    /**
     * Checks if dbxref properly maps in postgres
     * @param jtupl json file object with information
     * @param connection connection to postgres
     * @return dbxref to dbxref_id map
     */
    static Map<DbAccessionVersion, String> generateDbxrefToDb_idMap(JsonTuple jtupl, Connection connection) {
        StringBuilder query = new StringBuilder("SELECT db.name AS db, dbxref.accession, dbxref.version, dbxref_id " +
                "FROM dbxref JOIN db USING(db_id) JOIN (VALUES");
        List<String> unregisteredDbName = new ArrayList<String>();
        List<DbAccessionVersion> newAccession = new ArrayList<DbAccessionVersion>();
        for (String dbxref : jtupl.analysis.dbxref) {
            DbAccessionVersion tokens = BedFileConvertor.makeDbAccessionVersion(dbxref);
            checkForDbxrefInformationForUpdate(tokens, connection, unregisteredDbName, newAccession);
            query.append("(?,?,?),");
        }
        if (unregisteredDbName.size() > 0) {
            throw new IllegalArgumentException("There are unregistered dbnames in dbxrefList from JsonFile. " +
                    unregisteredDbName.toString());
        }
        if (newAccession.size() > 0) {
            System.out.println("New accessions are added: " + newAccession.toString());
        }
        query.setLength(query.length() - 1);
        query.append(") AS temp(db,accession,version) ON(db.name = temp.db AND dbxref.accession " +
                "= temp.accession AND dbxref.version = temp.version);");
        try {
            PreparedStatement preparedState = connection.prepareStatement(query.toString());
            int i = 0;
            for (String dbvalue : jtupl.analysis.dbxref) {
                DbAccessionVersion DAV = BedFileConvertor.makeDbAccessionVersion(dbvalue);
                DAV.assignStrToPeparedStatement(i, preparedState);
                i++;
            }
            ResultSet result = preparedState.executeQuery();
            HashMap<DbAccessionVersion, String> dbxrefTodbId = new HashMap<DbAccessionVersion, String>();
            Set<String> corruptresult = new HashSet<String>();
            while (result.next()) {
                DbAccessionVersion key = new DbAccessionVersion(
                        result.getString("db"),
                        result.getString("accession"),
                        result.getString("version"));
                if (dbxrefTodbId.containsKey(key) &&
                        !dbxrefTodbId.get(key).equals(result.getString("dbxref_id"))) {
                    corruptresult.add(key + " - " + result.getString("dbxref_id"));
                }
                dbxrefTodbId.put(key, result.getString("dbxref_id"));
            }

            if (corruptresult.size() > 0) {
                throw new IllegalArgumentException("More than one dbxref_id are connected to one or more data entry.\n" +
                        "Erroneous data tuples: " + corruptresult.toString());
            }
            if (jtupl.analysis.dbxref.size() != dbxrefTodbId.size()) {
                Set<String> listOfMissingDbxref = new HashSet<String>();
                for (String featureid : jtupl.analysis.dbxref) {
                    if (!dbxrefTodbId.containsKey(featureid)) {
                        listOfMissingDbxref.add(featureid);
                    }
                }
                throw new IllegalArgumentException("Some of the dbxref are not mapped by database.\n" +
                        "The dbxref are:" + listOfMissingDbxref);
            }
            return dbxrefTodbId;
        } catch (SQLException e){
            throw new IllegalArgumentException("SQL exception for dbxref to db_id sanity check! " + e);
        }
    }

    /**
     * Write paper's dbxref if paper's dbxref does not exit
     * @param DAV paper's db.name, accession, version object
     * @param connection connection to postgres
     * @param unregisteredDbName name of db.name that does not exist in postgres
     * @param newAccessions new accessions that are added
     */
    private static void checkForDbxrefInformationForUpdate(DbAccessionVersion DAV, Connection connection,
                                                           List<String> unregisteredDbName, List<DbAccessionVersion> newAccessions) {
        Map<String, Integer> dbnameToIDMap = new HashMap<String, Integer>();
        if (checkIfDbNameExist(DAV.db, connection, unregisteredDbName, dbnameToIDMap)) {
            checkIfDbxrefAccessionExistAndAdd(DAV, connection, newAccessions, dbnameToIDMap);
        }
    }


    /**
     * checks if particular db is present in postgres and maps dbname to db_id
     * @param dbname dbname
     * @param connection connection to postgres
     * @param unregisteredDbName list that contains all of missing dbnames
     * @param dbnameTodbIDMap dbname to db_id map
     * @return true if dbname is present
     */
    private static boolean checkIfDbNameExist(String dbname, Connection connection, List<String> unregisteredDbName, Map<String, Integer> dbnameTodbIDMap) {
        StringBuilder sb = new StringBuilder("SELECT db_id from db where db.name = ?;");
        try {
            PreparedStatement prestatement = connection.prepareStatement(sb.toString());
            prestatement.setString(1, dbname);
            ResultSet result = prestatement.executeQuery();
            if (result.next()) {
                dbnameTodbIDMap.put(dbname, result.getInt("db_id"));
                return true;
            }
        } catch (SQLException e) {
            System.err.println("SQL exception occured while checking db name for analysis.dbxref. " + e);
        }
        unregisteredDbName.add(dbname);
        return false;
    }

    /**
     * Checks if dbxref exist and add one if not exist in postgres
     * @param DAV DB.name, accession, version object to be checked
     * @param connection connection to postgres
     * @param newAccessions newly added accessions in this function
     * @param dbnameTodbIDMap dbname to id map to be updated as new accession is added.
     */
    private static void checkIfDbxrefAccessionExistAndAdd(DbAccessionVersion DAV,
                                                          Connection connection, List<DbAccessionVersion> newAccessions,
                                                          Map<String, Integer> dbnameTodbIDMap) {
        StringBuilder sb = new StringBuilder("SELECT accession, version from db join dbxref using (db_id) " +
                "where db.name = ? and dbxref.accession = ? AND dbxref.version = ?;");
        try {
            PreparedStatement prestatement = connection.prepareStatement(sb.toString());
            DAV.assignStrToPeparedStatement(0, prestatement);
            ResultSet result = prestatement.executeQuery();
            if (result.next()) {
                return;
            }
        } catch (SQLException e){
            System.err.println("SQL exception occured while checking dbxref accession for analysis.dbxref. " + e);
        }
        try {
            PreparedStatement prestatement = connection.prepareStatement(
                    "INSERT INTO dbxref (db_id, accession, version) VALUES (?,?,?);");
            prestatement.setInt(1, dbnameTodbIDMap.get(DAV.db));
            prestatement.setString(2, DAV.accession);
            prestatement.setString(3, DAV.version);
            prestatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL exception occured while inserting dbxref accession for analysis.dbxref. " + e);
        }
        newAccessions.add(DAV);
    }


    /**
     * From the given map, the function looks for key that has matching value of db, accession, and version. If there is
     * no match, returns null.
     * @param map Map to look into to find key that has matching value of db.name, accession, version in json file.
     * @param db db.name
     * @param accession dbxref.accession
     * @param version dbxref.version
     * @return key of type or sourceFeature value. Null for cvproperty.
     */
    static String getMatchingKey(Map<String, String> map, String db, String accession, String version) {
        for (String mapKey : map.keySet()) {
            DbAccessionVersion srcfTupl = BedFileConvertor.makeDbAccessionVersion(map.get(mapKey));
            if (srcfTupl.equals(new DbAccessionVersion(db, accession, version))) {
                return mapKey;
            }
        }
        return null;
    }


    /**
     * Find key of cvproperty values that has matching db.name, accession, version values from json file.
     * @param jtupl json object that contains cvprop json file.
     * @param db db.name
     * @param accession dbxref.accession
     * @param version dbxref.version
     * @return key of cvprop values mapped in json file.
     */
    private static String getMatchingKeyForCv(JsonTuple jtupl, String db, String accession, String version) {
        for (String firstKey : jtupl.analysis.cvprop.experimentMap.keySet()) {
            for (String secondKey : jtupl.analysis.cvprop.experimentMap.get(firstKey).tupleMap.keySet()) {
                for (String tempValue : jtupl.analysis.cvprop.experimentMap.get(firstKey).tupleMap.get(secondKey)) {
                    DbAccessionVersion tokens = BedFileConvertor.makeDbAccessionVersion(tempValue);
                    if (tokens.equals(new DbAccessionVersion(db, accession, version))) {
                        return tempValue.trim();
                    }
                }
            }
        }
        return null;
    }
}

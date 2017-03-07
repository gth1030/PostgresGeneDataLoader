

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.postgresql.core.Utils.escapeIdentifier;
import static org.postgresql.core.Utils.escapeLiteral;

/**
 * Created by kitae on 5/16/16.
 */
public class PostgresDataUploader {


    /**
     * Update postgres with data.
     * @param connection connection to postgres
     * @param jtupl json file object with information
     */
    public static void performFileUpload(Connection connection, JsonTuple jtupl) throws TransactionFailedException {
        Queue<String> featureIDs = assignFeatureID(connection);
        Reader featureReader = createReaderForDataUpload(BedFileConvertor.featureQue, featureColumns,
                new Boolean[] {false, false, true, false, false, false});
        Reader featurelocReader = createReaderForDataUpload(BedFileConvertor.featureLocQue,
                featurelocColumns, new Boolean[] {false, false, false, false, false, false, false, false});
        writeOnDatabaseFromMemory(featureTableName, featureColumns, featureReader, connection);
        writeOnDatabaseFromMemory(featurelocTableName, featurelocColumns, featurelocReader, connection);
        if (jtupl.columns.containsKey("FEATURE_DBXREF")){
            writeFeature_dbxref(BedFileConvertor.featureQue, connection);
        }
        if (jtupl.analysis != null) {
            String analysisID = insertAnalysisToDatabase(jtupl, connection);
            Reader analysisFeatureReader = generateAnalysisFeatureReader(jtupl, analysisID, analysisFeatureColumn, BedFileConvertor.featureQue);
            insertToDatabase(analysisFeatureReader, connection, analysisFeatureQuery[1], analysisFeatureQuery[0]);
            if (jtupl.analysis.property != null) {
                Reader analysisPropReader = generateAnalysisPropReader(jtupl, BedFileConvertor.cvnameTocvID, analysisID);
                insertToDatabase(analysisPropReader, connection, analysisPropQuery[1], analysisPropQuery[0]);
            }
            if (jtupl.analysis.cvprop != null) {
                Reader analysisCVPropReader = generateAnalysisCVPropReader(jtupl, analysisID);
                insertToDatabase(analysisCVPropReader, connection, analysisCvPropQuery[1], analysisCvPropQuery[0]);
            }
            if (jtupl.analysis.dbxref != null) {
                Reader analysisDbxrefReader = generateAnalysisDbxrefReader(jtupl, analysisID, BedFileConvertor.dbxrefToDb_idMap);
                insertToDatabase(analysisDbxrefReader, connection, analysis_DBxrefQuery[1], analysis_DBxrefQuery[0]);
            }
        }
    }

    /**
     * Assigns feature id to each feature tuples.
     * @param connection connection to postgres
     * @return list of feature_ids generated
     */
    private static Queue<String> assignFeatureID(Connection connection) throws TransactionFailedException {
        Queue<String> featureIDs = requestFeatureIds(BedFileConvertor.featureQue.size(), connection);
        Queue<String> returnedFeatureID = new ArrayDeque<String>();
        while (!featureIDs.isEmpty()) {
            String newFeatureId = featureIDs.poll();
            FeatureTuple featureTuple = BedFileConvertor.featureQue.poll();
            featureTuple.feature_id = newFeatureId;
            BedFileConvertor.featureQue.add(featureTuple);
            returnedFeatureID.add(newFeatureId);
        }
        return returnedFeatureID;
    }

    /**
     * Generate reader for feature and featureloc table update.
     * @param que que with feature or featueloc tuple
     * @param columnNames column names of table to update
     * @param isString specify if each column is string column or not
     * @return reader with feature or featureloc information
     */
    private static Reader createReaderForDataUpload(Queue<? extends TupleInterface> que, String[] columnNames, Boolean[] isString) {
        StringBuilder sb = new StringBuilder();
        Iterator<? extends TupleInterface> iterator = que.iterator();
        TupleInterface line = null;
        while (iterator.hasNext()) {
            line = iterator.next();
            List<String> stringList = new ArrayList<String>();
            for (int i = 0; i < columnNames.length; i++) {
                stringList.add(TupleInterface.getProperValue(columnNames[i], line));
            }
            writeTuple(sb, stringList.toArray(new String[stringList.size()]), isString);
        }
        return new BufferedReader(new StringReader(sb.toString()));
    }

    /**
     * Write to postgres from a reader. Used for feature and featureloc table.
     * @param tablename name of table to update.
     * @param columnNames names of columns in given table
     * @param reader reader with data to write
     * @param connection connection to postgres
     */
    private static void writeOnDatabaseFromMemory(String tablename, String[] columnNames,
                                                Reader reader, Connection connection) throws TransactionFailedException {
        try {
            CopyManager copyManager = new CopyManager((BaseConnection)connection);
            StringBuilder sb = new StringBuilder("COPY ");
            sb.append(tablename);
            sb.append(" (");
            Boolean isFrist = true;
            for (String column : columnNames) {
                if (isFrist) {sb.append(column); isFrist = false; continue;}
                sb.append("," + column);
            }
            sb.append(") FROM STDIN WITH (FORMAT CSV, HEADER false);");
            copyManager.copyIn(sb.toString(), reader);
            System.out.println(tablename + " has been uploaded.");
        } catch (SQLException e) {
            System.err.println("Exception happened for " + tablename + " table upload. " + e);
            BedFileConvertor.forceRollBack(connection);
        } catch (IOException e) {
            System.err.println(e + " for " + tablename);
        }

    }


    /**
     * Request feature_ids from database.
     * @param numOfFeatureIDs number of feature ids to request
     * @param connection connection to postgres
     * @return Queue that contains a list of new feature ids
     */
    private static Queue<String> requestFeatureIds(int numOfFeatureIDs, Connection connection) throws TransactionFailedException {
        Queue<String> listOfFeatureNum = null;
        try {
            PreparedStatement prestatement = connection.prepareStatement(
                    "SELECT nextval('feature_feature_id_seq') FROM generate_series(1,?);");
            prestatement.setInt(1, numOfFeatureIDs);
            ResultSet result = prestatement.executeQuery();
            listOfFeatureNum = new ArrayDeque<String>() ;
            while (result.next()) {
                listOfFeatureNum.add(result.getString(1));
            }
        } catch (SQLException e) {
            System.err.println("Failed to request extra feature IDs : " + e);
            BedFileConvertor.forceRollBack(connection);
        }
        return listOfFeatureNum;
    }


    /**
     * Insert analysis into database and returns analysis_id
     * @param jtupl tuple with analysis information
     * @param connection connection to Postgres
     * @return analysis_id assigned by postgres
     */
    private static String insertAnalysisToDatabase(JsonTuple jtupl, Connection connection) throws TransactionFailedException {
        String timeExecuted = "";
        String timeExecutedValue = "";
        if (jtupl.analysis.download_date != null) {
            timeExecuted = ", timeexecuted";
            timeExecutedValue = ",?";
        }
        String query = "INSERT INTO analysis (name, description, program, programversion, algorithm, " +
                "sourcename, sourceversion, sourceuri, sourcepart" + timeExecuted + ") VALUES (?,?,?,?,?,?,?,?,?" + timeExecutedValue +") RETURNING analysis_id;";
        try {
            PreparedStatement prestatement = connection.prepareStatement(query);
            prestatement.setString(1, jtupl.analysis.type);
            prestatement.setString(2, jtupl.analysis.notes);
            prestatement.setString(3, jtupl.analysis.program.get(0));
            prestatement.setString(4, jtupl.analysis.program.get(1));
            prestatement.setString(5, jtupl.analysis.algorithm);
            prestatement.setString(6, jtupl.analysis.source.get(0));
            prestatement.setString(7, jtupl.analysis.source.get(1));
            prestatement.setString(8, jtupl.analysis.dataurl);
            prestatement.setString(9, jtupl.analysis.sourcePart);
            if (jtupl.analysis.download_date != null) {
                prestatement.setDate(10, java.sql.Date.valueOf(jtupl.analysis.download_date));
            }
            ResultSet result = prestatement.executeQuery();
            System.out.println("Analysis has been uploaded.");
            result.next();
            return result.getString(1);
        } catch (SQLException e) {
            System.err.println("Insertion for analysis failed! " + e);
            BedFileConvertor.forceRollBack(connection);
        }
        return null;
    }


    /**
     * Generate reader for analysisprop
     * @param jTuple tuple with information
     * @param cvNameToIDmap maps cvname to CvName
     * @param analysisID analysis_id
     * @return reader with analysisprop information
     */
    private static Reader generateAnalysisPropReader(JsonTuple jTuple, Map<DbCvname, String> cvNameToIDmap, String analysisID) {
        StringBuilder sb = new StringBuilder();
        for (String key : jTuple.analysis.property.experimentMap.keySet()) {
            JsonTuple.Analysis.Experiment experiment = jTuple.analysis.property.experimentMap.get(key);
            for (String tupleMapKey : experiment.tupleMap.keySet()) {
                for (String value : experiment.tupleMap.get(tupleMapKey)) {
                    if (!jTuple.analysispropRankCounter.containsKey(new DbCvname(key, tupleMapKey))) {
                        jTuple.analysispropRankCounter.put(new DbCvname(key, tupleMapKey), 0);
                    }
                    String[] tuple = new String[]{analysisID, cvNameToIDmap.get(new DbCvname(key, tupleMapKey)),
                            value, Integer.toString(jTuple.analysispropRankCounter.get(new DbCvname(key, tupleMapKey)))};
                    writeTuple(sb, tuple, new Boolean[]{false, false, true, false});
                    jTuple.analysispropRankCounter.put(new DbCvname(key, tupleMapKey),
                            jTuple.analysispropRankCounter.get(new DbCvname(key, tupleMapKey)) + 1);
                }
            }
        }
        return new BufferedReader(new StringReader(sb.toString()));
    }


    /**
     * generate reader for cvprop insertion for postgres.
     * @param jtupl tuple with cvprop information
     * @param analysisID analysis_id
     * @return reader with cvprop information
     */
    private static Reader generateAnalysisCVPropReader(JsonTuple jtupl, String analysisID) {
        StringBuilder sb = new StringBuilder();
        for (String key : jtupl.analysis.cvprop.experimentMap.keySet()) {
            JsonTuple.Analysis.Experiment experiment = jtupl.analysis.cvprop.experimentMap.get(key);
            for (String tupleMapKey : experiment.tupleMap.keySet()) {
                for (String value : experiment.tupleMap.get(tupleMapKey)) {
                    if (!jtupl.analysiscvPropRankCounter.containsKey(new DbCvname(key, tupleMapKey))) {
                        jtupl.analysiscvPropRankCounter.put(new DbCvname(key, tupleMapKey), 0);
                    }
                    String[] tuple = new String[]{analysisID,
                            BedFileConvertor.cvnameTocvID.get(new DbCvname(key, tupleMapKey)),
                            BedFileConvertor.dbxrefToCvtermMap.get(value.trim()),
                            Integer.toString(jtupl.analysiscvPropRankCounter.get(new DbCvname(key, tupleMapKey)))};
                    writeTuple(sb, tuple, new Boolean[]{false, false, true, false});
                    jtupl.analysiscvPropRankCounter.put(new DbCvname(key, tupleMapKey),
                            jtupl.analysiscvPropRankCounter.get(new DbCvname(key, tupleMapKey)) + 1);
                }
            }
        }
        return new BufferedReader(new StringReader(sb.toString()));
    }


    /**
     * generate analysisFeature reader.
     * @param jtupl tuple with analysisFeature information
     * @param analysisID analysis_id
     * @param columnList name of columns for analysisFeature
     * @param featureIDs List of FeatureIDs
     * @return reader with analysisfeature information
     */
    private static Reader generateAnalysisFeatureReader(JsonTuple jtupl, String analysisID, String[] columnList, Queue<FeatureTuple> featureIDs) {
        StringBuilder sb = new StringBuilder();
        for (FeatureTuple feature : featureIDs) {
            String[] tuple = new String[8];
            tuple[0] = analysisID; tuple[1] = feature.feature_id;
            int i = 2;
            for (String column : columnList) {
                tuple[i] = (jtupl.columns.containsKey(column)) ? feature.getProperFeatureAnalysisVal(column) : "";
                i++;
            }
        writeTuple(sb, tuple, new Boolean[] {false, false, false, false, false, false, false, false});
        }
        return new BufferedReader(new StringReader(sb.toString()));
    }


    /**
     * Write to postgres from reader.
     * @param reader reader with information to write.
     * @param connection connection to postgres
     * @param query operation query for postgres
     * @param tableName name of table for error output
     */
    private static void insertToDatabase(Reader reader, Connection connection, String query, String tableName) throws TransactionFailedException {
        try {
            CopyManager copyManager = new CopyManager((BaseConnection) connection);
            copyManager.copyIn(query, reader);
            System.out.println(tableName + " has been uploaded.");
        }catch (SQLException e) {
            System.err.println("Insertion for " + tableName + " failed! " + e);
            BedFileConvertor.forceRollBack(connection);
        }catch (IOException e) {
            System.err.println("IOException for " + tableName + " update! " + e);
            BedFileConvertor.forceRollBack(connection);
        }
    }

    /**
     * generate reader for analysisDbxref
     * @param jtupl tuple with analysis_dbxref information
     * @param analysisID analysis_id
     * @param dbxrefToDbId maps dbxref to db_id
     * @return reader with analysis_Dbxref information
     */
    private static Reader generateAnalysisDbxrefReader(JsonTuple jtupl, String analysisID, Map<DbAccessionVersion, String> dbxrefToDbId) {
        StringBuilder sb = new StringBuilder();
        for (String tuple : jtupl.analysis.dbxref) {
            String[] tokens = new String[2];
            tokens[0] = analysisID;
            DbAccessionVersion DAV = BedFileConvertor.makeDbAccessionVersion(tuple);
            tokens[1] = dbxrefToDbId.get(DAV);
            writeTuple(sb, tokens, new Boolean[] {false, false});
        }
        return new BufferedReader(new StringReader(sb.toString()));
    }


    /**
     * Write a line to SB for reader formation with escapeIdentifier to preven sql injection.
     * @param sb StringBuilder
     * @param tuple each sigment of string to write on SB
     * @param isString boolean identifier to define if given tuple segment is a string or not
     */
    private static void writeTuple(StringBuilder sb, String[] tuple, Boolean[] isString) {
        for (int i = 0; i < tuple.length; i++) {
            try {
                if (isString[i]) {
                    escapeIdentifier(sb, tuple[i]);
                } else {
                    escapeLiteral(sb, tuple[i], true);
                }
            } catch (SQLException e) {
                System.err.println("Casting failed for " + tuple[i] + ".");
                e.printStackTrace();
            }
            sb.append(",");
        }
        sb.setLength(sb.length() - 1);
        sb.append("\n");
    }

    static void handleCommitRollback(Connection connection, Boolean isCommitTrue) throws TransactionFailedException {
        if (isCommitTrue) {
            try {
                PreparedStatement transactionRollback = connection.prepareStatement("COMMIT;");
                transactionRollback.executeUpdate();
                System.out.println("Transaction is committed!");
            } catch (SQLException e) {
                System.err.println("FATAL:Transaction COMMIT failed! " + e);
                try {
                    PreparedStatement transactionRollback = connection.prepareStatement("ROLLBACK;");
                    transactionRollback.executeUpdate();
                    System.out.println("Database rolled back!");
                    throw new TransactionFailedException("");
                } catch (SQLException e1) {
                    System.err.println("FATAL:Transaction rollback failed! " + e1 + "\nConnection forced to close.");
                    try { connection.close();
                    } catch (SQLException e2) { e2.printStackTrace();
                    } finally { System.exit(1); }
                }
            }
        } else {
            try {
                PreparedStatement transactionRollback = connection.prepareStatement("ROLLBACK;");
                transactionRollback.executeUpdate();
                System.out.println("Database rolled back!");
            } catch (SQLException e1) {
                System.err.println("FATAL:Transaction rollback failed! " + e1 + "\nConnection forced to close.");
                try {
                    connection.close();
                } catch (SQLException e2) {
                    e2.printStackTrace();
                } finally {
                    System.exit(1);
                }
            }
        }
    }

    /**
     * Uploads dbxref and feature_dbxref values if feature_dbxref column is present in Json file.
     * @param featureQue queue that contains all feature values with dbxref_id for each tuple
     * @param connection connection to postgres server.
     * @throws TransactionFailedException
     */
    public static void writeFeature_dbxref(Queue<FeatureTuple> featureQue, Connection connection) throws TransactionFailedException {
        System.out.println("Loading dbxref & feature_dbxref.");
        writeDbxref(featureQue, connection);
        writeFeatureDbxref(featureQue, connection);
    }

    /**
     * Uploads feature_dbxref values to database.
     * @param featureQue queue that contains all feature values with dbxref_id for each tuple
     * @param connection connection to postgres server.
     * @throws TransactionFailedException
     */
    public static void writeFeatureDbxref(Queue<FeatureTuple> featureQue, Connection connection) throws TransactionFailedException {
        Reader featureDbxrefReader = createReaderForDataUpload(featureQue, new String[]{"feature_dbxref_id", "feature_id", "dbxref_id"},
                new Boolean[]{false, false, false});
        writeOnDatabaseFromMemory("feature_dbxref", new String[]{"feature_dbxref_id", "feature_id", "dbxref_id"},
                featureDbxrefReader, connection);
    }

    /**
     * Checks if each dbxref value for feature is present in the database, and if not present, create one and upload to
     * database. Also feature_dbxref_id is assigned to each feature at this stage
     * @param featureQue queue of featureTuples that have dbxref values
     * @param connection connection to database
     */
    public static void writeDbxref(Queue<FeatureTuple> featureQue, Connection connection)
            throws TransactionFailedException {
        Set<DbxrefTuple> dbxrefToLoadSet = new HashSet<DbxrefTuple>();
        Map<String, String> presentDbMaps = new HashMap<String, String>();

        //Used to reverseMap dbxref_id from dbxrefTuple to each feature with efficiency!
        Map<DbxrefTuple, Set<FeatureTuple>> reversefeatureMap = new HashMap<DbxrefTuple, Set<FeatureTuple>>();
        try {
            PreparedStatement prestatement = connection.prepareStatement(
                    "SELECT nextval('feature_dbxref_feature_dbxref_id_seq') FROM generate_series(1,?);");
            prestatement.setInt(1, featureQue.size());
            ResultSet featureDbxrefIDs = prestatement.executeQuery();
            featureDbxrefIDs.next();
            for (FeatureTuple feature : featureQue) {
                feature.feature_dbxref_id = featureDbxrefIDs.getString(1);
                String dbName = feature.featureDbxrefFullAcc.split(":")[0];
                if (!presentDbMaps.containsKey(dbName)) {
                    presentDbMaps.put(dbName, getDb_id(dbName, connection));
                }
                StringBuilder query = new StringBuilder();
                query.append("Select dbxref_id from dbxref where db_id = ? and accession = ?");
                PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
                preparedStatement.setString(1, presentDbMaps.get(dbName));
                preparedStatement.setString(2, feature.featureDbxrefFullAcc.split(":")[1]);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    feature.dbxref_id = resultSet.getString("dbxref_id");
                }
                if (feature.dbxref_id == null || feature.dbxref_id.length() == 0) {
                    DbxrefTuple key = new DbxrefTuple(presentDbMaps.get(dbName),
                            feature.featureDbxrefFullAcc.split(":")[1]);
                    dbxrefToLoadSet.add(key);
                    if (!reversefeatureMap.containsKey(key))
                        reversefeatureMap.put(key, new HashSet<FeatureTuple>());
                    reversefeatureMap.get(key).add(feature);
                }
            }

            prestatement = connection.prepareStatement(
                    "SELECT nextval('dbxref_dbxref_id_seq') FROM generate_series(1,?);");
            prestatement.setInt(1, dbxrefToLoadSet.size());
            ResultSet result = prestatement.executeQuery();
            StringBuilder sb = new StringBuilder();
            for (DbxrefTuple dbxref : dbxrefToLoadSet) {
                result.next();
                dbxref.dbxref_id = result.getString(1);
                writeTuple(sb, new String[]{dbxref.db_id, dbxref.accession, result.getString(1)},
                        new Boolean[]{false,true,false});
            }
            BufferedReader dbxrefUploadReader = new BufferedReader(new StringReader(sb.toString()));
            writeOnDatabaseFromMemory("dbxref", new String[]{"db_id", "accession", "dbxref_id"}, dbxrefUploadReader, connection);
        } catch (SQLException e) {
            System.err.println("SQL exception while uploading dbxref for features!");
            e.printStackTrace();
        }
    }

    /**
     * Check if db_id is present on the database.
     * @param dbName name of db to search from database to find db_id
     * @param connection connection to postgres server
     * @return db_id
     * @throws TransactionFailedException
     */
    public static String getDb_id(String dbName, Connection connection) throws TransactionFailedException {
        String db_id = "";
        try {
            StringBuilder query = new StringBuilder();
            query.append("Select db_id from db where name = ?");
            PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
            preparedStatement.setString(1, dbName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                if(db_id.length() != 0) {
                    throw new TransactionFailedException("Error: more than one db is detected with db name.");
                }
                db_id = resultSet.getString("db_id");
            }
            if (db_id.length() == 0) {
                throw new TransactionFailedException("Error: db name does not exist! db name = " +
                        dbName);
            }
        } catch (SQLException e) {
            System.err.println("SQL exception while uploading feature_dbxref and dbxref for features");
            e.printStackTrace();
        }
        return db_id;
    }


    /* Column names for each table */
    private static final String[] featureColumns = new String[] {"feature_id", "organism_id", "uniquename", "seqlen", "type_id", "is_analysis"};
    private static final String[] featurelocColumns = new String[] {"feature_id", "srcfeature_id", "fmin", "fmax", "rank", "locgroup", "phase", "strand"};
    private static final String[] analysisFeatureColumn = new String[] {"RAWSCORE", "NORMSCORE", "SIGNIFICANCE", "IDENTITY", "PERCENTILE", "RANK"};

    private static final String featureTableName = "feature";
    private static final String featurelocTableName = "featureloc";

    /* query for data upload */
    private static final String[] analysis_DBxrefQuery = new String[] {"analysis_dbxref", "COPY analysis_dbxref(analysis_id, dbxref_id) FROM STDIN WITH (FORMAT CSV, HEADER false);"};
    private static final String[] analysisFeatureQuery = new String[] {"analysisfeature", "COPY analysisfeature(analysis_id,feature_id,rawscore,normscore,significance,identity,percentile,rank) FROM STDIN WITH (FORMAT CSV, HEADER false);"};
    private static final String[] analysisCvPropQuery = new String[] {"analysiscvprop", "COPY analysiscvprop(analysis_id, type_id, value_id, rank) FROM STDIN WITH (FORMAT CSV, HEADER false);"};
    private static final String[] analysisPropQuery = new String[] {"analysisprop", "COPY analysisprop(analysis_id, type_id, value, rank) FROM STDIN WITH (FORMAT CSV, HEADER false);"};

}

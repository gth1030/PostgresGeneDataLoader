

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
        Queue<FeatureTuple> tempFeatureTuples = new PriorityQueue<FeatureTuple>(BedFileConvertor.featureQue);
        Reader featureReader = createReaderForDataUpload(BedFileConvertor.featureQue, featureColumns,
                new Boolean[] {false, false, true, false, false, false});
        Reader featurelocReader = createReaderForDataUpload(BedFileConvertor.featureLocQue,
                featurelocColumns, new Boolean[] {false, false, false, false, false, false});
        writeOnDatabaseFromMemory(featureTableName, featureColumns, featureReader, connection);
        writeOnDatabaseFromMemory(featurelocTableName, featurelocColumns, featurelocReader, connection);
        if (jtupl.analysis != null) {
            String analysisID = insertAnalysisToDatabase(jtupl, connection);
            Reader analysisFeatureReader = generateAnalysisFeatureReader(jtupl, analysisID, analysisFeatureColumn, tempFeatureTuples);
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
        TupleInterface line = null;
        while ((line = que.poll()) != null) {
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
     * @param jtupl tuple with information
     * @param cvNameToIDmap maps cvname to CvName
     * @param analysisID analysis_id
     * @return reader with analysisprop information
     */
    private static Reader generateAnalysisPropReader(JsonTuple jtupl, Map<DbCvname, String> cvNameToIDmap, String analysisID) {
        StringBuilder sb = new StringBuilder();
        for (String key : jtupl.analysis.property.experimentMap.keySet()) {
            JsonTuple.Analysis.Experiment experiment = jtupl.analysis.property.experimentMap.get(key);
            for (String tupleMapKey : experiment.tupleMap.keySet()) {
                for (String value : experiment.tupleMap.get(tupleMapKey)) {
                    if (!jtupl.analysispropRankCounter.containsKey(new DbCvname(key, tupleMapKey))) {
                        jtupl.analysispropRankCounter.put(new DbCvname(key, tupleMapKey), 0);
                    }
                    String[] tuple = new String[]{analysisID, cvNameToIDmap.get(new DbCvname(key, tupleMapKey)),
                            value, Integer.toString(jtupl.analysispropRankCounter.get(new DbCvname(key, tupleMapKey)))};
                    writeTuple(sb, tuple, new Boolean[]{false, false, true, false});
                    jtupl.analysispropRankCounter.put(new DbCvname(key, tupleMapKey),
                            jtupl.analysispropRankCounter.get(new DbCvname(key, tupleMapKey)) + 1);
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
                    try {
                        connection.close();
                    } catch (SQLException e2) {
                        e2.printStackTrace();
                    } finally {
                        System.exit(1);
                    }
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

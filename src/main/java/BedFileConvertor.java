import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by kitae on 5/11/16.
 */
public class BedFileConvertor {

    /* Find user's directory, and reads config file. */
    private BedFileConvertor() {
    }

    /* Provides Singleton Design of the program */
    static BedFileConvertor getInstance() {
        return bedFileConvertor;
    }

    /* Creates initial setup. */
    public static void setUp() {
        URL location = BedFileConvertor.class.getProtectionDomain().getCodeSource().getLocation();
        File file = new File(location.getFile());
        String configFilePath = file.getParentFile().getParentFile().getAbsolutePath() + "/configuration.config";
        ArgumentHandler.fetchConfigFile(configFilePath);
    }


    public static void main(String[] args) {
        System.out.println("Bed file convertor starting . . .");
        setUp();
        List<String> listOfJason = ArgumentHandler.parseArguments(args);
        connectToPostgres(portDBConnectionString, userName, password);
        System.out.println("System connected to the database!");
        //Begin reading jsonFiles
        try {
            Set<String> failedJsonFileList = new HashSet<String>();
            for (String jsonFile : listOfJason) {
                try {
                    System.out.println("Reading jsonfile. filename = " + jsonFile);
                    JsonTuple jtupl = JsonTuple.readJsonFormatter("./" + jsonFile);
                    // Begin reading bedfiles in the jsonFile.
                    try {
                        PreparedStatement transactionStart = connection.prepareStatement("BEGIN;");
                        transactionStart.executeUpdate();
                    } catch (SQLException e) {
                        System.err.print("Transaction 'BEGIN' failed! " + e);
                        throw new TransactionFailedException("");
                    }
                    for (String bedfilePath : jtupl.bedFile) {
                        initializeJsonFile(jtupl);
                        if (!jtupl.isTypeCondensible) {
                            readBedFileWriteQueue(jtupl, new File(bedFileFolder + "/" + bedfilePath));
                        } else {
                            readBedFileWirteQueueForCondensed(getInstance(), jtupl, new File(bedFileFolder + "/" + bedfilePath));
                        }
                        if (isVerbose) {
                            System.out.println("dbxref to feature_id and oraganism is " + dbxrefToFeatureOrganismMap);
                            System.out.println("dbxref to cvterm map is " + dbxrefToCvtermMap);
                            System.out.println("newly generated features are " + featureQue);
                        }
                        PostgresDataUploader.performFileUpload(connection, jtupl);
                        PostgresDataUploader.handleCommitRollback(connection, isCommitTrue);
                    }
                } catch (TransactionFailedException e){
                    failedJsonFileList.add(jsonFile);
                }
            }
            if (failedJsonFileList.size() > 0)
                System.out.println("Json files failed to upload are: " + failedJsonFileList.toString());
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            try {
                PreparedStatement transactionRollback = connection.prepareStatement("ROLLBACK;");
                transactionRollback.executeUpdate();
                System.err.println("Database rolled back!");
            } catch (SQLException er) {
                System.err.println("FATAL:Transaction rollback failed! " + er + "\nThe server might not have been properly rolled back!");
            }
            System.exit(1);
        }
        System.out.println("Uploader is properly closing!");
        System.exit(0);
    }



    /**
     * The function writes featureQueue and featurelocqueue for dataupload. For each bedfile tuple, feature, and featureloc is
     * created.
     * @param file path of bedfile to be read.
     */
    private static void readBedFileWriteQueue(JsonTuple jtupl, File file) {
        BufferedReader br = null;
        file = new File(JsonTuple.checkForGZedFileForUnzip(file.getPath()));
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            Set<String> corruptedType = new HashSet<String>();
            Set<String> corruptedSrcfeature = new HashSet<String>();
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                checkIfTypeAndSrcfeatureExist(tokens, jtupl, corruptedType, corruptedSrcfeature);
                FeatureTuple feature = new FeatureTuple(tokens, jtupl);
                featureQue.add(feature);
                featureLocQue.add(new FeatureLocTuple(tokens, 0, jtupl, feature));
            }
            if (corruptedType.size() != 0 || corruptedSrcfeature.size() != 0) {
                throw new IllegalArgumentException("There are unrecognized type and/or sourcefeature." +
                        "\nUnrecognized type is: " + corruptedType.toString() +
                        "\nUnrecognized sourceFeature is: " + corruptedSrcfeature.toString());
            }
        } catch (IOException e) {
            System.err.println("IOException occured while reading file name = " + file.getName());
            e.printStackTrace();
        }
    }


    /**
     * The function builds featureQueue and featureLocQueue for condensible format of data. If json file is missing all
     * optional components of columns' attribute, type can be condensed into number of types in the json file instead of
     * generating type for each bedfile tuple.
     * @param file path of bedfile to be read.
     */
    private static void readBedFileWirteQueueForCondensed(BedFileConvertor bedconvertor, JsonTuple jtupl, File file) {
        BufferedReader br = null;
        file = new File(JsonTuple.checkForGZedFileForUnzip(file.getPath()));
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            Set<String> corruptedType = new HashSet<String>();
            Set<String> corruptedSrcfeature = new HashSet<String>();
            for (String key : jtupl.type.keySet()) {
                featureQue.add(new FeatureTuple(jtupl, dbxrefToCvtermMap.get(key)));
            }
            Map<String, Integer> rankCounter = new HashMap<String, Integer> ();
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                checkIfTypeAndSrcfeatureExist(tokens, jtupl, corruptedType, corruptedSrcfeature);
                FeatureTuple tempFeature = getFeatureTupleAndGenerateNewIfNeeded(jtupl, bedconvertor,
                        jtupl.getTypeID(tokens),
                        dbxrefToFeatureOrganismMap.get(tokens[jtupl.getSrcFeatureIndex()]).organism_id // <- organism_id of the srcfeature
                );
                featureLocQue.add(new FeatureLocTuple(tokens, FeatureLocTuple.getRank(rankCounter, jtupl, tokens),
                        jtupl, tempFeature));
            }
            FeatureTuple.filterUnusedFeature_Condensed(featureQue);
            if (corruptedType.size() != 0 || corruptedSrcfeature.size() != 0) {
                throw new IllegalArgumentException("There are unrecognized type and/or sourcefeature by database." +
                        "\nUnrecognized type is: " + corruptedType.toString() +
                        "\nUnrecognized sourceFeature is: " + corruptedSrcfeature.toString());
            }
        } catch (IOException e) {
            System.err.println("IOException occured while reading file name = " + file.getName());
        }
    }



    /**
     * The function checks if on a rare case, there is more than one organism in the given bedfile. If there are more than one organisms, then the function
     * generates new feature tuple. If not, the function is used to assign organism IDs for each feature, and unused feature tuple is recognized from null value.
     */
    private static FeatureTuple getFeatureTupleAndGenerateNewIfNeeded(JsonTuple jtupl, BedFileConvertor bedconvertor,
                                                                      String type_idKey, String organism_id) {
        for (FeatureTuple featureTuple : featureQue) {
            if (dbxrefToCvtermMap.get(type_idKey).equals(featureTuple.type_id)) {
                if (featureTuple.organism_id == null || featureTuple.organism_id.equals(organism_id))
                    featureTuple.organism_id = organism_id;
                return featureTuple;
            }
        }
        FeatureTuple newTuple = new FeatureTuple(jtupl, dbxrefToCvtermMap.get(type_idKey));
        featureQue.add(newTuple);
        newTuple.organism_id = organism_id;
        return newTuple;
    }


    /* Checks if json file is correctly formatted with enough information. Also creates maps from database by matching
    * dbNames and other variables from the genestation to obtain, feature ID, Organism ID, Cvterm ID, and so on. */
    private static void initializeJsonFile(JsonTuple jTuple) {
        featureQue = new ArrayDeque<FeatureTuple>();
        featureLocQue = new ArrayDeque<FeatureLocTuple>();
        if (jTuple.columns.containsKey("NAME") || jTuple.baseName != null) {
            System.out.println("Json File has depreciated feature of name or base name that is not used!\nThe program proceeds without using them.");
        }
        try {
            dbxrefToFeatureOrganismMap = JsonFileSanityCheck.generateDbxrefToFeatureOrganismMap(jTuple, connection);
            dbxrefToCvtermMap = JsonFileSanityCheck.generateDbxrefToCvtermMap(jTuple, connection);
            if (jTuple.analysis != null) {
                if (jTuple.analysis.cvprop != null || jTuple.analysis.property != null) {
                    cvnameTocvID = JsonFileSanityCheck.generateCvnameToCvIDMap(jTuple, connection);
                }
                if (jTuple.analysis.dbxref != null) {
                    dbxrefToDb_idMap = JsonFileSanityCheck.generateDbxrefToDb_idMap(jTuple, connection);
                }
            }
        } catch (SQLException e) {
            System.err.println("SQLException occured during sanity check for files : " + "\nError Message: " + e);
            e.printStackTrace();
        }
    }


    /**
     * Establishes connection to postgres.
     *
     * @param portDbname database information for connection, the format is "jdbc:postgresql://hostname:port/dbname"
     * @param username   username for the database
     * @param password   password for the database
     */
    private static void connectToPostgres(String portDbname, String username, String password) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("JDBC driver missing!");
        }
        try {
            connection = DriverManager.getConnection(portDbname, username, password);
        } catch (SQLException e) {
            System.err.println("Postgres connection has Failed!");
            System.exit(1);
        }
        if (connection == null) {
            System.err.println("Postgres connection has failed!");
            System.exit(1);
        }
    }


    /**
     * Add version at the end if version is missing. Also return string in separate manner.
     * @param str String of name:accession:version
     * @return String array of name, accession, version.
     */
    static DbAccessionVersion makeDbAccessionVersion(String str) {
        String[] tokens = str.split(":");
        if (tokens.length == 2) {
            return new DbAccessionVersion(tokens[0], tokens[1]);
        }
        throw new IllegalArgumentException("Incorrect value is passed to makeDbAccessionVersion");
    }


    /**
     * Checks srcfeature and type value of each data tuple is mapped in JsonFile and record
     * unmapped values in corrupt recorder
     * @param dataTuple a single row of data tuple
     * @param jTuple JsonTuple object that contains information for data file.
     * @param corruptTypeRecorder container for corrupted type name
     * @param corruptSrcFeatureRecorder container for corrupted srcfeature name
     */
    private static void checkIfTypeAndSrcfeatureExist(String[] dataTuple, JsonTuple jTuple,
                                                   Set<String> corruptTypeRecorder,
                                                      Set<String> corruptSrcFeatureRecorder) {
        String type_id = jTuple.getTypeID(dataTuple);
        if (!dbxrefToCvtermMap.containsKey(type_id)) {
            corruptTypeRecorder.add(type_id);
        }
        String srcfeature_key = dataTuple[jTuple.getSrcFeatureIndex()];
        if (!dbxrefToFeatureOrganismMap.containsKey(srcfeature_key)) {
            corruptSrcFeatureRecorder.add(srcfeature_key);
        }
    }




    /** Enforce safe termination of transaction when error is encountered. **/
    static void forceRollBack(Connection connection) throws TransactionFailedException {
        try {
            PreparedStatement transactionRollback = connection.prepareStatement("ROLLBACK;");
            transactionRollback.executeUpdate();
            System.out.println("Database rolled back due to the error!");
        } catch (SQLException e) {
            System.err.println("FATAL:Transaction rollback failed! " + e);
            System.err.println("It has to be done manually to avoid issues!!!!");
            System.exit(1);
        } finally {
            throw new TransactionFailedException("");
        }
    }

    static void setPassword(String Inputpassword) {
        password = Inputpassword;
    }

    static void setUsername(String InputUserName) {
        userName = InputUserName;
    }

    static void setConnectionString(String connectionString) {
        portDBConnectionString = connectionString;
    }

    /* Singleton instance of the program */
    private static BedFileConvertor bedFileConvertor = new BedFileConvertor();
    /* Current working directory of user to find location of Json files and bedfiles. */
    private static final String bedFileFolder = System.getProperty("user.dir");;
    /* Database connection protocol. */
    private static String portDBConnectionString = "";
    /* Username for postgres */
    private static String userName = "";
    /* password for postgres */
    private static String password = "";
    /* Map with key = db.name : dbxref.accession : dbxref.version(no space in between); and value = FeatureOrganism with feature_id & organism_id*/
    static Map<String, FeatureOrganism> dbxrefToFeatureOrganismMap;
    /* Map with key = db.name : dbxref.accession : dbxref.version; and value = cvterm_id  for type and cvproperty if exist. */
    static Map<String, String> dbxrefToCvtermMap;
    /* Map with key = cv.name : cvterm.name and value = cvterm_id*/
    static Map<DbCvname, String> cvnameTocvID;
    /* Map with dbxref to DB_id */
    static Map<DbAccessionVersion, String> dbxrefToDb_idMap;
    /* Queue holds each feature to be inserted into database. */
    static Queue<FeatureTuple> featureQue;
    /* Queue holds each featureLocTuple to be inserted into database. Each bedfile tuple produce one featureLoc*/
    static Queue<FeatureLocTuple> featureLocQue;
    /* Connection to database; is null if no connection. */
    static Connection connection = null;
    /* variable to print more specification. */
    static boolean isVerbose;
    /* variable to decide if commit is needed. */
    static boolean isCommitTrue;


}


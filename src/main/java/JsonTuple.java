import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by kitae on 5/11/16.
 */
public class JsonTuple {


    /** read json file and parse it. **/
    static JsonTuple readJsonFormatter(String filePath) {

        Gson gson = new GsonBuilder().registerTypeAdapter(JsonTuple.class, new JsonTuple.typeDeserializer()).create();
        JsonReader jreader = null;
        try {
            jreader = new JsonReader(new FileReader(filePath));
        } catch (IOException e) {
            System.err.println("IOException occured for reading json file. : ");
            e.printStackTrace();
        }
        JsonTuple jtupl = gson.fromJson(jreader, JsonTuple.class);
        checkJsonFileValuePresence(jtupl);
        if (!jtupl.columns.containsKey("FEATURE") && !jtupl.columns.containsKey("RAWSCORE")
                && !jtupl.columns.containsKey("NORMSCORE") && !jtupl.columns.containsKey("IDENTITY")
                && !jtupl.columns.containsKey("SIGNIFICANCE") && !jtupl.columns.containsKey("RANK")
                && !jtupl.columns.containsKey("PERCENTILE") && !jtupl.columns.containsKey("NAME")) {
            jtupl.isTypeCondensible = true;
        }
        return jtupl;
    }

    /** performs overall format sanity check for json file. **/
    private static void checkJsonFileValuePresence(JsonTuple jtupl) {
        //Column check
        if (jtupl.columns == null) {
            throw new IllegalArgumentException("COLUMNS must exist in json file.");
        }
        if (!jtupl.columns.containsKey("SRCFEATURE") || !jtupl.columns.containsKey("START") ||
                !jtupl.columns.containsKey("END")) {
            throw new IllegalArgumentException("For columns, SRCFEATURE, START, and END must exist in json file.");
        }
        //Index, SRCFEATURE, and Bedfile check
        if (jtupl.index == null || jtupl.srcfeature == null || jtupl. bedFile == null ||
                Integer.parseInt(jtupl.index) > 2 || Integer.parseInt(jtupl.index) < 0) {
            throw new IllegalArgumentException("Index, Srcfeature, and bedfile must exist in proper format.");
        }
        //Type check. Type must exist and if column:type is missing, then Type must have a single entry to apply every
        //entry in the file same type.
        if (jtupl.type == null || !jtupl.columns.containsKey("TYPE") && jtupl.type.size() != 1) {
            throw new IllegalArgumentException("Type must exist in json file. Also, if column:type is missing, " +
                    "type can only have one entry to apply same type to every value in the file.");
        }
        //Analysis check.
        if (jtupl.analysis != null && (jtupl.analysis.uploader == null || jtupl.analysis.program == null ||
                jtupl.analysis.type == null || jtupl.analysis.source == null)) {
            throw new IllegalArgumentException("For analysis, uploader, type, program, and source must exist in json file.");
        }
    }


    /** helps serialization of type in json file. **/
    private void setTypeValues(String value) {
        type = new HashMap<String, String>();
        type.put(value, value);
    }


    /* checks if a file is needed to be unziped and unzip it. Return new unzipped file path. */
    static String checkForGZedFileForUnzip(String filePath) {
        String fileName = new File(filePath).getName();
        String fileExtension = "";
        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0) {
            fileExtension = fileName.substring(fileName.lastIndexOf(".") + 1);
        }
        if (fileExtension.equals("gz")) {
            unGunzipFile(filePath, filePath.substring(0, filePath.lastIndexOf(".")));
            return filePath.substring(0, filePath.lastIndexOf((".")));
        }
        return filePath;
    }


    /* Unzip file. */
    private static void unGunzipFile(String compressedFile, String decompressedFile) {
        byte[] buffer = new byte[1024];
        try {
            FileInputStream fileIn = new FileInputStream(compressedFile);
            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);
            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);
            int bytes_read;
            while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
                fileOutputStream.write(buffer, 0, bytes_read);
            }
            gZIPInputStream.close();
            fileOutputStream.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }


    /**
     * Deserialize Json file into JsonTuple.
     */
    public static class typeDeserializer implements JsonDeserializer<JsonTuple> {
        @Override
        public JsonTuple deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            Gson gson = new Gson();
            JsonTuple jTuple = gson.fromJson(json, JsonTuple.class);
            JsonObject jsonObject = json.getAsJsonObject();
            if (jsonObject.has("TYPE")) {
                JsonElement elem = jsonObject.get("TYPE");
                try {
                    jTuple.type = gson.fromJson(elem, new TypeToken<Map<String, String>>() {}.getType());
                } catch (JsonSyntaxException e) {
                    if (elem != null && !elem.isJsonNull() && jTuple.type == null) {
                        String valuesString = elem.getAsString();
                        if (valuesString != null) {
                            jTuple.setTypeValues(valuesString);
                        }
                    }
                }
            }
            if (jsonObject.has("BEDFILE")) {
                JsonElement elem = jsonObject.get("BEDFILE");
                try {
                    jTuple.bedFile = gson.fromJson(elem, new TypeToken<List<String>>() {}.getType());
                } catch (JsonSyntaxException e) {
                    if (elem != null && !elem.isJsonNull()) {
                        String valuesString = elem.getAsString();
                        if (valuesString != null) {
                            jTuple.bedFile.add(valuesString);
                        }
                    }
                }
            }
            if (jsonObject.has("ANALYSIS")) {
                JsonObject temp = jsonObject.getAsJsonObject("ANALYSIS");
                if (temp.has("DBXREF")) {
                    JsonElement elem = temp.get("DBXREF");
                    try {
                        jTuple.analysis.dbxref = gson.fromJson(elem, new TypeToken<List<String>>() {}.getType());
                    } catch (JsonSyntaxException e) {
                        if (elem != null && !elem.isJsonNull()) {
                            String valuesString = elem.getAsString();
                            if (valuesString != null) {
                                jTuple.analysis.dbxref = new ArrayList<String>();
                                jTuple.analysis.dbxref.add(valuesString);
                            }
                        }
                    }
                }
                if (temp.has("SOURCE")) {
                    JsonElement elem = temp.get("SOURCE");
                    try {
                        jTuple.analysis.source = gson.fromJson(elem, new TypeToken<List<String>>() {}.getType());
                    } catch (JsonSyntaxException e) {
                        if (elem != null && !elem.isJsonNull()) {
                            String valuesString = elem.getAsString();
                            if (valuesString != null) {
                                jTuple.analysis.source = new ArrayList<String>();
                                jTuple.analysis.source.add(valuesString);
                                jTuple.analysis.source.add("");
                            }
                        }
                    }
                    if (jTuple.analysis.source.size() != 2) {
                        throw new IllegalArgumentException("The size of Analysis.source can not exceed 2.");
                    }
                }
                try {
                    temp = temp.getAsJsonObject("CVPROPERTY");
                } catch (NullPointerException ignored) {
                }
                if (temp != null) {
                    jTuple.analysis.cvprop = jTuple.analysis.new nestedProp();
                    JsonObject temp2 = null;
                    for (Map.Entry<String, JsonElement> entry : temp.entrySet()) {
                        temp2 = entry.getValue().getAsJsonObject();
                        String cvpropKey = entry.getKey();
                        jTuple.analysis.cvprop.experimentMap.put(cvpropKey, jTuple.analysis.new Experiment());
                        JsonElement elem = null;
                        for (Map.Entry<String, JsonElement> innerentry : temp2.entrySet()) {
                            elem = innerentry.getValue();
                            String key = innerentry.getKey();
                            List<String> tempList = null;
                            try {
                                tempList = new Gson().fromJson(elem.getAsJsonArray(),
                                        new TypeToken<List<String>>() {
                                        }.getType());
                            } catch (IllegalStateException e) {
                                if (elem != null && !elem.isJsonNull()) {
                                    String valuesString = elem.getAsString();
                                    if (valuesString != null) {
                                        tempList = new ArrayList<String>();
                                        tempList.add(valuesString);
                                    }
                                }
                            }
                            jTuple.analysis.cvprop.experimentMap.get(cvpropKey).tupleMap.put(key, tempList);
                        }
                    }
                }
                temp = jsonObject.getAsJsonObject("ANALYSIS");
                try {
                    temp = temp.getAsJsonObject("PROPERTY");
                } catch (NullPointerException e) {
                }
                if (temp != null) {
                    jTuple.analysis.property = jTuple.analysis.new nestedProp();
                    JsonObject temp2 = null;
                    for (Map.Entry<String, JsonElement> entry : temp.entrySet()) {
                        temp2 = entry.getValue().getAsJsonObject();
                        String cvpropKey = entry.getKey();
                        jTuple.analysis.property.experimentMap.put(cvpropKey, jTuple.analysis.new Experiment());
                        JsonElement elem = null;
                        for (Map.Entry<String, JsonElement> innerentry : temp2.entrySet()) {
                            elem = innerentry.getValue();
                            String key = innerentry.getKey();
                            List<String> tempList = null;
                            try {
                                tempList = new Gson().fromJson(elem.getAsJsonArray(),
                                        new TypeToken<List<String>>() {
                                        }.getType());
                            } catch (IllegalStateException e) {
                                if (elem != null && !elem.isJsonNull()) {
                                    String valuesString = elem.getAsString();
                                    if (valuesString != null) {
                                        tempList = new ArrayList<String>();
                                        tempList.add(valuesString);
                                    }
                                }
                            }
                            jTuple.analysis.property.experimentMap.get(cvpropKey).tupleMap.put(key, tempList);
                        }
                    }
                }
            }
            return jTuple ;
        }
    }

    /**
     * Generate unique for each data point of feature if unique name is not provided.
     * @param type_val key for a type column
     * @return a unique name
     */
    String generateName(String type_val) {
        if (!typeCounterForUniqueName.containsKey(type_val)) {
            typeCounterForUniqueName.put(type_val, 0);
        }
        String name = analysis.source.get(0);
        if (analysis.source.size() >= 2) {
            name += "(" + analysis.source.get(1) + ")";
        }
        name += "_" + analysis.sourcePart;
        name += "_" + type_val;
        name += "[" + (typeCounterForUniqueName.get(type_val) + 1) + "]";
        typeCounterForUniqueName.put(type_val, typeCounterForUniqueName.get(type_val) + 1);
        return name;
    }

    /* return TypeID of dataTuple */
    public String getTypeID(String[] dataTuple) {
        return (columns.containsKey("TYPE")) ? dataTuple[getTypeIndex()] : type.get(type.keySet().toArray()[0]);
    }

    /* return index number for type value in data tuple */
    public int getTypeIndex() {
        if (columns.containsKey("TYPE")) {
            return Integer.parseInt(columns.get("TYPE"));
        }
        throw new IllegalArgumentException("Type column does not exist for this file, but type column index is requested!");
    }

    /* return index number for srcfeature value in data tuple */
    public int getSrcFeatureIndex() {
        return Integer.parseInt(columns.get("SRCFEATURE"));
    }


    /**
     * Parse analysis section of json file.
     */
    class Analysis {
        @SerializedName("UPLOADER")
        String uploader;
        @SerializedName("TYPE")
        String type;
        @SerializedName("ALGORITHM")
        String algorithm;
        @SerializedName("SOURCEPART")
        String sourcePart;
        @SerializedName("DATAURL")
        String dataurl;
        @SerializedName("DOWNLOAD_DATE")
        String download_date = null;
        @SerializedName("NOTES")
        String notes;

        @SerializedName("PROGRAM")
        List<String> program;
        List<String> dbxref = null;
        List<String> source;

        nestedProp cvprop = null;
        nestedProp property = null;

        /* Class to provide internal data structure for cvprop and prop data. */
        class nestedProp {
            Map<String, Experiment> experimentMap = new HashMap<String, Experiment>();
        }
        /* Class to provide internal data structure for maps inside cvprop and prop data. */
        class Experiment {
            Map<String, List<String>> tupleMap = new HashMap<String, List<String>>();
        }
    }


    /* Analysis part of json file. */
    @SerializedName("ANALYSIS")
    Analysis analysis = null;
    /* BaseName that becomes part of unique name */
    @SerializedName("BASENAME")
    String baseName;
    /* indexing method of start, and end. */
    @SerializedName("INDEX")
    String index;
    /* Column specification for tsv file. */
    @SerializedName("COLUMNS")
    Map<String, String> columns;
    /* List of Srcfeature from json file. */
    @SerializedName("SRCFEATURE")
    Map<String, String> srcfeature;
    /* List of types from json file. */
    Map<String, String> type;
    /* Counter for analysisPropRanks */
    Map<DbCvname, Integer> analysispropRankCounter = new HashMap<DbCvname, Integer>();
    /* Counter to assign rank for analysiscvprop table. */
    Map<DbCvname, Integer> analysiscvPropRankCounter = new HashMap<DbCvname, Integer>();
    /* Counter for each type to generate unique name of each feature. Map is Map<type_val, count> */
    Map<String, Integer> typeCounterForUniqueName = new HashMap<String, Integer>();
    /* list of paths to bed files that belong to one json file. */
    List<String> bedFile = new ArrayList<String>();
    /* True if type is condensible. Type is condensible if proper meta analysis data is missing. */
    boolean isTypeCondensible = false;


    

}

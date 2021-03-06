import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * Created by kitae on 10/25/16.
 */
public class ArgumentHandler {

    /**
     * Parse arguments in correct form.
     * @param args program arguments.
     * @return List of json file names to be processed.
     */
    static List<String> parseArguments(String[] args) {
        ArgumentParser argpharser = ArgumentParsers.newArgumentParser("jsonreader").
                description("Read arguments to receive commands and file names.");
        argpharser.addArgument("-c", "--commit").action(Arguments.storeTrue()).
                help("Commit to database when this is on. Without commit on, program will mock commit.");
        argpharser.addArgument("-v", "--verbose").action(Arguments.storeTrue()).
                help("Print out more detailed message for user.");
        argpharser.addArgument("-f", "--force").action(Arguments.storeTrue()).
        help("Force loader to proceed with unrecognized data tuples, and just ignore tuples with an errors.");
        argpharser.addArgument("fileNames").type(String.class).nargs("+")
                .help("list of json file names to phase through");
        List<String> listOfJason = null;
        try {
            Namespace nameSpace = argpharser.parseArgs(args);
            BedFileConvertor.isCommitTrue = nameSpace.get("commit");
            BedFileConvertor.isVerbose = nameSpace.get("verbose");
            BedFileConvertor.isForceCommit = nameSpace.get("force");
            listOfJason = nameSpace.get("fileNames");
        } catch (ArgumentParserException e) {
            argpharser.handleError(e);
            System.exit(1);
        }
        return listOfJason;
    }


    /**
     * Parse configuration file.
     * @param configFilePath path to configuration file.
     */
    public static void fetchConfigFile(String configFilePath) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File(configFilePath)));
            String line = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.charAt(0) == '/' && line.charAt(1) == '/') {
                    continue;
                }
                if (line.substring(0, 13).equals("loginFilePath")) {
                    String loginFilePath = null;
                    try {
                        loginFilePath = line.split("=")[1].trim();
                    } catch (IndexOutOfBoundsException e) {
                        continue;
                    }
                    BufferedReader loginFile = new BufferedReader(new FileReader(new File(loginFilePath)));
                    JsonParser parser = new JsonParser();
                    JsonElement elem = parser.parse(new JsonReader(loginFile));
                    JsonObject jsonObject = elem.getAsJsonObject();
                    BedFileConvertor.setUsername(jsonObject.get("user").getAsString());
                    BedFileConvertor.setPassword(jsonObject.get("password").getAsString());
                }
                if (line.substring(0, 16).equals("portDBConnection")) {
                    try {
                        BedFileConvertor.setConnectionString(line.split("=")[1].trim());
                    } catch (IndexOutOfBoundsException e) {
                        continue;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("IOException for reading configuration file! " + e);
            System.exit(1);
        }
    }
}

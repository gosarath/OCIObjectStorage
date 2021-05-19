import org.apache.commons.cli.*;

import java.io.*;
import java.util.Map;

public class oStoreAPI {

    public static void main(String[] args) throws IOException {

        Options options = new Options();

        Option objStoreMode = new Option("mo", "Mode", true, "Mode : Upload");
        objStoreMode.setRequired(true);
        options.addOption(objStoreMode);

        Option filePath = new Option("fp", "FilePath", true, "File Path");
        filePath.setRequired(true);
        options.addOption(filePath);

        Option configLoc = new Option("cl", "ConfigLoc", true, "Config file path");
        configLoc.setRequired(true);
        options.addOption(configLoc);

        Option bucket = new Option("bu", "Bucket", true, "ObjStore Bucket");
        bucket.setRequired(true);
        options.addOption(bucket);

        Option region = new Option("re", "Region", true, "ObjStore Region");
        region.setRequired(true);
        options.addOption(region);

        Option nameSpace = new Option("ns", "NameSpace", true, "ObjStore NameSpace");
        nameSpace.setRequired(true);
        options.addOption(nameSpace);

        Option file = new Option("ob", "Object", true, "ObjStore File");
        file.setRequired(true);
        options.addOption(file);

        CommandLineParser parser = (CommandLineParser) new org.apache.commons.cli.DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        String ConfigLoc = cmd.getOptionValue("ConfigLoc");
        String Region = cmd.getOptionValue("Region");
        String NameSpace = cmd.getOptionValue("NameSpace");
        String Bucket = cmd.getOptionValue("Bucket");
        String object = cmd.getOptionValue("Object");
        String mode = cmd.getOptionValue("Mode");
        String fPath = cmd.getOptionValue("FilePath");

        ObjStore oStore =  new ObjStore(ConfigLoc,Region,NameSpace,Bucket,object,fPath);

        if (mode.equalsIgnoreCase("up")){
            oStore.upload();
        } else if (mode.equalsIgnoreCase("de")) {
            oStore.delete();
        } else if (mode.equalsIgnoreCase("re")) {
            oStore.read();
        }


    }
}

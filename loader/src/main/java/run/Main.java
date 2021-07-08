package run;

import loader.PrefixEncoder;
import loader.DE9IMGeometriesRelationsLoader;
import loader.DictionaryEncoder;
import loader.TripleTableLoader;
import loader.VerticalPartitioningLoader;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * The Main class parses the CLI arguments and calls the executor.
 * <p>
 * Options: -h, --help prints the usage help message. -i, --input <file> HDFS
 * input path of the RDF graph. -o, --output <DBname> output database name. -s,
 * compute statistics
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class Main {

    private static String input_location;
    private static String relations_location;
    private static String de9im_output_location;
    private static String outputDB;
    private static String lpStrategies;
    private static String loj4jFileName = "log4j.properties";
    private static final Logger logger = Logger.getLogger("PRoST");
    private static boolean useStatistics = false;
    private static boolean dropDuplicates = true;
    private static boolean generateTT = false;
    private static boolean generateWPT = false;
    private static boolean generateVP = false;
    private static boolean generateExtVP = false;
    private static boolean generateDEC = false;
    private static boolean generatePEC = false;
    private static boolean cacherelations = false;
    
    // options for physical partitioning
    private static boolean ttPartitionedByPred = false;
    private static boolean ttPartitionedBySub = false;
    private static String statFile;
    private static String dictionaryFile;
    private static double thresholdExtVP = 0.25;
    private static String tripleTable = "triples";

    public static void main(final String[] args) throws Exception {
        final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
        final Properties props = new Properties();
        props.load(inStream);
        PropertyConfigurator.configure(props);
        long start = System.currentTimeMillis();
        /*
		 * Manage the CLI options
         */
        final CommandLineParser parser = new PosixParser();
        final Options options = new Options();

        final Option inputOpt = new Option("i", "input", true, "HDFS input path of the RDF graph.");
        inputOpt.setRequired(true);
        options.addOption(inputOpt);

        final Option geometriesDE9IM = new Option("de9im", "DE9IM", true, "HDFS path of the geometries DE9IM relations file.");
        geometriesDE9IM.setRequired(false);
        options.addOption(geometriesDE9IM);        
        
        final Option outputDE9IM = new Option("outde9im", "outDE9IM", true, "HDFS path of the file that will contain each spatial predicate number of rows(=true).");
        outputDE9IM.setRequired(false);
        options.addOption(outputDE9IM);   
        
        final Option outputOpt = new Option("o", "output", true, "Output database name.");
        outputOpt.setRequired(true);
        options.addOption(outputOpt);

        final Option statFileOpt = new Option("sf", "statisticsfile", true, "Statistics Filename.");
        statFileOpt.setRequired(true);
        options.addOption(statFileOpt);

        final Option dictFileOpt = new Option("df", "dictionaryfile", true, "Dictionary Filename.");
        dictFileOpt.setRequired(true);
        options.addOption(dictFileOpt);

        final Option lpOpt = new Option("lp", "logicalPartitionStrategies", true, "Logical Partition Strategy.");
        lpOpt.setRequired(false);
        options.addOption(lpOpt);

        final Option helpOpt = new Option("h", "help", false, "Print this help.");
        options.addOption(helpOpt);

        final Option statsOpt = new Option("s", "stats", false, "Flag to produce the statistics");
        options.addOption(statsOpt);

        final Option duplicatesOpt = new Option("dp", "dropDuplicates", true,
                "Option to remove duplicates from all logical partitioning tables.");
        options.addOption(duplicatesOpt);

        // Settings for physically partitioning some of the tables
        final Option ttpPartPredOpt = new Option("ttp", "ttPartitionedByPredicate", false,
                "To physically partition the Triple Table by predicate.");
        ttpPartPredOpt.setRequired(false);
        options.addOption(ttpPartPredOpt);

        final Option ttpPartSubOpt
                = new Option("tts", "ttPartitionedBySub", false, "To physically partition the Triple Table by subject.");
        ttpPartSubOpt.setRequired(false);
        options.addOption(ttpPartSubOpt);

        final Option wptPartSubOpt = new Option("wpts", "wptPartitionedBySub", false,
                "To physically partition the Wide Property Table by subject.");
        wptPartSubOpt.setRequired(false);
        options.addOption(wptPartSubOpt);

        final Option outputTripleTable = new Option("ott", "outTripleTable", true,
                "name of triple table output (only for tripleTable)");
        outputTripleTable.setRequired(false);
        options.addOption(outputTripleTable);

        final HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (final MissingOptionException e) {
            formatter.printHelp("JAR", "Load an RDF graph", options, "", true);
            return;
        } catch (final ParseException e) {
            e.printStackTrace();
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("JAR", "Load an RDF graph as Property Table using SparkSQL", options, "", true);
            return;
        }
        if (cmd.hasOption("input")) {
            input_location = cmd.getOptionValue("input");
            logger.info("Input path set to: " + input_location);
        }
        if (cmd.hasOption("DE9IM")) {
        	relations_location = cmd.getOptionValue("DE9IM");
            logger.info("Relations path set to: " + relations_location);
            cacherelations = true;
        }
        if (cmd.hasOption("outDE9IM")) {
        	de9im_output_location = cmd.getOptionValue("outDE9IM");
            logger.info("DE9IM output path set to: " + de9im_output_location);
        }
        if (cmd.hasOption("output")) {
            outputDB = cmd.getOptionValue("output");
            logger.info("Output database set to: " + outputDB);
        }
        if (cmd.hasOption("statisticsfile")) {
            statFile = cmd.getOptionValue("statisticsfile");
            logger.info("Statistics file: " + statFile);
        }
        if (cmd.hasOption("dictionaryfile")) {
            dictionaryFile = cmd.getOptionValue("dictionaryfile");
            logger.info("Dictionary file: " + dictionaryFile);
        }
        //addition for DH
        if (cmd.hasOption("outTripleTable")) {
            tripleTable = cmd.getOptionValue("outTripleTable");
        }

        // default if a logical partition is not specified is: TT, WPT, and VP.
        if (!cmd.hasOption("logicalPartitionStrategies")) {
            generateTT = true;
            generateWPT = true;
            generateVP = true;
            generateDEC = true;
            logger.info("Logical strategy used: TT + WPT + VP");
        } else {
            lpStrategies = cmd.getOptionValue("logicalPartitionStrategies");

            final List<String> strategies = Arrays.asList(lpStrategies.split(","));

            if (strategies.contains("TT")) {
                generateTT = true;
                logger.info("Logical strategy used: TT");
            }
            if (strategies.contains("VP")) {
                generateVP = true;
                logger.info("Logical strategy used: VP");
            }
            if (strategies.contains("EXTVP")) {
                generateVP = true;
                generateExtVP = true;
                logger.info("Logical strategy used: VP");
            }
            if (strategies.contains("DEC")) {
                generateDEC = true;
                logger.info("Logical strategy used: DEC");

            }
            if (strategies.contains("PEC")) {
                generatePEC = true;
                logger.info("Logical strategy used: PEC");
            }
        }

        // Relevant for physical partitioning
        if (cmd.hasOption("ttPartitionedByPredicate") && cmd.hasOption("ttPartitionedBySub")) {
            logger.error("Triple table cannot be partitioned by both subject and predicate.");
            return;
        }
        if (cmd.hasOption("ttPartitionedByPredicate")) {
            ttPartitionedByPred = true;
            logger.info("Triple Table will be partitioned by predicate.");
        }
        if (cmd.hasOption("ttPartitionedBySub")) {
            ttPartitionedBySub = true;
            logger.info("Triple Table will be partitioned by subject.");
        }

        // The defaulf value of dropDuplicates is true, so this needs to be
        // changed just in case user sets it as false.
        if (cmd.hasOption("dropDuplicates")) {
            final String dropDuplicateValue = cmd.getOptionValue("dropDuplicates");
            if (dropDuplicateValue.compareTo("false") == 0) {
                dropDuplicates = false;
            }
            logger.info("Duplicates won't be removed from the tables.");
        }

        if (cmd.hasOption("stats")) {
            useStatistics = true;
            logger.info("Statistics active!");

            if (!generateVP) {
                logger.info("Logical strategy activated: VP. VP needed to generate statistics.");
                generateVP = true;
            }
        }

        // Set the loader from the inputFile to the outputDB
        final SparkSession spark = SparkSession.builder().appName("PRoST-Loader")
                //.master("local[*]")
                .enableHiveSupport().getOrCreate();

        // Removing previous instances of the database in case a database with
        // the same name already exists.
        // In this case a new database with the same name will be created.
        //spark.sql("DROP DATABASE IF EXISTS " + outputDB + " CASCADE");
        long startTime;
        long executionTime;

        if (generateTT) {
            startTime = System.currentTimeMillis();
            final TripleTableLoader tt_loader = new TripleTableLoader(input_location, outputDB, tripleTable, spark,
                    ttPartitionedBySub, ttPartitionedByPred, dropDuplicates);
            tt_loader.load();

            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
        }

        if (generateDEC) {
            startTime = System.currentTimeMillis();
            final DictionaryEncoder de_loader = new DictionaryEncoder(input_location, outputDB, tripleTable, spark,
                    null);
            de_loader.load();

            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
        }

        if (generatePEC) {
            startTime = System.currentTimeMillis();
            final PrefixEncoder pe_loader = new PrefixEncoder(input_location, outputDB, tripleTable, spark,
                    null);
            pe_loader.load();

            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
        }

        if (cacherelations) {
            startTime = System.currentTimeMillis();
            logger.info(relations_location);
            final DE9IMGeometriesRelationsLoader DE9IM_loader
                    = new DE9IMGeometriesRelationsLoader(input_location, outputDB, spark,relations_location,de9im_output_location);
            DE9IM_loader.load();
            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to create geometries relations tables: " + String.valueOf(executionTime));        	
        }        
        
        if (generateVP) {
            startTime = System.currentTimeMillis();
            final VerticalPartitioningLoader vp_loader
                    = new VerticalPartitioningLoader(input_location, outputDB, tripleTable, spark, useStatistics, statFile,
                            dictionaryFile, generateExtVP, thresholdExtVP);
            vp_loader.load();
            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to build the Vertical partitioning: " + String.valueOf(executionTime));
        }
        
        logger.info("Total time: " + (System.currentTimeMillis() - start));

    }
}

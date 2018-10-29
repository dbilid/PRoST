package run;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import executor.Executor;
import joinTree.JoinTree;
import translator.Stats;
import translator.Translator;

/**
 * The Main class parses the CLI arguments and calls the translator and the executor.
 *
 * @author Matteo Cossu
 */
public class Main {

	private static final Logger logger = Logger.getLogger("PRoST");

	private static String inputFile;
	private static String outputFile;
	private static String statsFileName = "";
	private static String database_name;
	private static int treeWidth = -1;
	private static int minimumGroupSize = -1;
	private static boolean useVerticalPartitioning = false;
	private static boolean usePropertyTable = false;
	private static boolean useInversePropertyTable = false;
	private static boolean useJoinedPropertyTable = false;
	// if triples have to be grouped when using property table
	private static boolean isGrouping = true;
	
	private static boolean benchmarkMode = false;
	private static String benchmark_file;
	private static String loj4jFileName = "log4j.properties";

	public static void main(final String[] args) throws IOException {
		final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
		final Properties props = new Properties();
		props.load(inStream);
		PropertyConfigurator.configure(props);

		/*
		 * Manage the CLI options
		 */
		final CommandLineParser parser = new PosixParser();
		final Options options = new Options();
		final Option inputOpt = new Option("i", "input", true, "Input file with the SPARQL query.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);
		final Option outputOpt = new Option("o", "output", true, "Path for the results in HDFS.");
		options.addOption(outputOpt);
		final Option statOpt = new Option("s", "stats", true, "File with statistics (required)");
		options.addOption(statOpt);
		statOpt.setRequired(true);
		final Option databaseOpt = new Option("d", "DB", true, "Database containing the VP tables and the PT.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);
		final Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);
		final Option widthOpt = new Option("w", "width", true, "The maximum Tree width");
		options.addOption(widthOpt);
		final Option lpOpt = new Option("lp", "logicalPartitionStrategies", true, "Logical Partition Strategy.");
		lpOpt.setRequired(false);
		options.addOption(lpOpt);

		final Option disableGroupingOpt = new Option("dg", "disablesGrouping", false, "Disables grouping of triple patterns when using " + "WPT, IWPT, or JWPT models");
		disableGroupingOpt.setRequired(false);
		options.addOption(disableGroupingOpt);

		final Option benchmarkOpt = new Option("t", "times", true, "Save the time results in a csv file.");
		options.addOption(benchmarkOpt);

		final Option groupSizeOpt = new Option("g", "groupSize", true, "Minimum Group Size for Wide Property Table nodes");
		options.addOption(groupSizeOpt);

		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (final MissingOptionException e) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		} catch (final ParseException e) {
			e.printStackTrace();
		}
		if (cmd.hasOption("help")) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		}
		if (cmd.hasOption("input")) {
			inputFile = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("output")) {
			outputFile = cmd.getOptionValue("output");
			logger.info("Output file set to:" + outputFile);
		}
		if (cmd.hasOption("stats")) {
			statsFileName = cmd.getOptionValue("stats");
		}
		if (cmd.hasOption("width")) {
			treeWidth = Integer.valueOf(cmd.getOptionValue("width"));
			logger.info("Maximum tree width is set to " + String.valueOf(treeWidth));
		}
		if (cmd.hasOption("DB")) {
			database_name = cmd.getOptionValue("DB");
		}
		if (cmd.hasOption("times")) {
			benchmarkMode = true;
			benchmark_file = cmd.getOptionValue("times");
		}

		if (cmd.hasOption("groupSize")) {
			minimumGroupSize = Integer.valueOf(cmd.getOptionValue("groupSize"));
			logger.info("Minimum Group Size set to " + String.valueOf(minimumGroupSize));
		}

		// default if a logical partition is not specified is: WPT, and VP.
		if (!cmd.hasOption("logicalPartitionStrategies")) {
			useVerticalPartitioning = true;
			usePropertyTable = true;
			logger.info("Default strategies used: WPT + VP");
		} else {
			final String lpStrategies = cmd.getOptionValue("logicalPartitionStrategies");
			final List<String> strategies = Arrays.asList(lpStrategies.toUpperCase().split(","));

			if (strategies.contains("VP")) {
				useVerticalPartitioning = true;
				logger.info("Logical strategy used: VP");
			}
			if (strategies.contains("WPT")) {
				usePropertyTable = true;
				logger.info("Logical strategy used: WPT");
			}
			if (strategies.contains("IWPT")) {
				useInversePropertyTable = true;
				logger.info("Logical strategy used: IWPT");
			}
			if (strategies.contains("JWPT")) {
				useJoinedPropertyTable = true;
				logger.info("Logical strategy used: JWPT");
			}
		}

		if (cmd.hasOption("disablesGrouping")) {
			isGrouping = false;
			logger.info("Grouping of multiple triples is disabled.");
		}

		//validate input
		if (useJoinedPropertyTable && (useInversePropertyTable || usePropertyTable)) {
			useInversePropertyTable = false;
			usePropertyTable = false;
			logger.info("WPT and IWPT disabled. WPT and IWPT are not used when JWPT is enabled");
		}
		if (!isGrouping && minimumGroupSize != 1) {
			minimumGroupSize = 1;
			logger.info("Minimum group size set to 1 when grouping is disabled");
		}
		if (!useVerticalPartitioning && minimumGroupSize != 1) {
			minimumGroupSize = 1;
			logger.info("Minimum group size set to 1 when VP is disabled");
		}
		if (!isGrouping && useInversePropertyTable && usePropertyTable){
			useInversePropertyTable = false;
			logger.info("Disabled IWPT. Not used when grouping is disabled and WPT is enabled");
		}

		// create a singleton parsing a file with statistics
		Stats.getInstance().parseStats(statsFileName);

		final File file = new File(inputFile);

		// single file
		if (file.isFile()) {
			// translation phase
			final JoinTree translatedQuery = translateSingleQuery(inputFile, treeWidth);
			// System.out.println("****************************************************");
			// System.out.println(translatedQuery);
			// System.out.println("****************************************************");

			// execution phase
			final Executor executor = new Executor(translatedQuery, database_name);
			if (outputFile != null) {
				executor.setOutputFile(outputFile);
			}
			executor.execute();
			
			if (benchmarkMode) {
				executor.saveResultsCsv(benchmark_file);
			}
		} else if (file.isDirectory()) {
			// set of queries
			// empty executor to initialize Spark
			final Executor executor = new Executor(null, database_name);

			if (benchmarkMode) {
				// executor.cacheTables();
				executeBatch(random_sample(file.list(), 3), executor);
				executor.clearQueryTimes();
			}

			// if the path is a directory execute every files inside
			executeBatch(file.list(), executor);

			if (benchmarkMode) {
				executor.saveResultsCsv(benchmark_file);
			}

		}
		logger.error("The input file is not set correctly or contains errors");
	}

	private static JoinTree translateSingleQuery(final String query, final int width) {
		final Translator translator = new Translator(query, width);
		translator.setUseVerticalPartitioning(useVerticalPartitioning);
		translator.setUsePropertyTable(usePropertyTable);
		translator.setUseInversePropertyTable(useInversePropertyTable);
		translator.setUseJoinedPropertyTable(useJoinedPropertyTable);
		translator.setIsGrouping(isGrouping);
		if (minimumGroupSize != -1) {
			translator.setMinimumGroupSize(minimumGroupSize);
		}
		return translator.translateQuery();
	}

	private static void executeBatch(final String[] queries, final Executor executor) {
		for (final String fname : queries) {
			logger.info("Starting: " + fname);

			// translation phase
			final JoinTree translatedQuery = translateSingleQuery(inputFile + "/" + fname, treeWidth);

			// execution phase
			executor.setQueryTree(translatedQuery);
			executor.execute();
		}
	}

	private static String[] random_sample(final String[] queries, final int k) {
		final String[] sample = new String[k];
		for (int i = 0; i < sample.length; i++) {
			final int randomIndex = ThreadLocalRandom.current().nextInt(0, queries.length);
			sample[i] = queries[randomIndex];
		}
		return sample;
	}
}

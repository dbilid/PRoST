package loader2;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import run2.Main;

/**
 * This abstract class define the parameters and methods for loading an RDF
 * graph into HDFS using Spark SQL.
 *
 * Knows where input data is located in HDFS, which Hive DB to create/use and
 * which Spark Session to use
 *
 * @author Theofilos Ioannidis
 */
public abstract class Loader {

    // CLASS MEMBERS
    protected static final Logger logger = Logger.getLogger(Main.appName);

    // DATA MEMBERS    
    protected SparkSession spark;
    protected String dbName;
    protected boolean flagDBExists;
    protected boolean flagCreateDB;
    protected String hdfsInputDir;
    protected boolean useHiveQL_TableCreation;

    // CONSTRUCTORS
    // 1. Detailed/Base Constructor
    public Loader(final SparkSession spark, final String dbName,
            boolean flagDBExists, boolean flagCreateDB,
            final String hdfsInputDir, final boolean useHiveQL_TableCreation) {
        this.spark = spark;
        this.dbName = dbName;
        this.flagDBExists = flagDBExists;
        this.flagCreateDB = flagCreateDB;
        this.hdfsInputDir = hdfsInputDir;
        this.useHiveQL_TableCreation = useHiveQL_TableCreation;
        createUseDB();
    }

    // DATA ACCESSORS
    public boolean isFlagDBExists() {
        return flagDBExists;
    }

    public boolean isFlagCreateDB() {
        return flagCreateDB;
    }

    public boolean isUseHiveQL_TableCreation() {
        return useHiveQL_TableCreation;
    }

    // METHODS
    public abstract void load() throws Exception;

    /**
     * Replace all not allowed characters of a DB column name by an
     * underscore("_") and return a valid DB column name. The datastore accepts
     * only characters in the range [a-zA-Z0-9_]
     *
     * @param columnName column name that will be validated and fixed
     * @return name of a DB column
     */
    protected String getValidHiveName(final String columnName) {
        return columnName.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
    }

    /**
     * Remove all the tables indicated as parameter.
     *
     * @param tableNames the names of the tables that will be removed
     * @return
     */
    protected void dropTables(final String... tableNames) {
        for (final String tb : tableNames) {
            spark.sql("DROP TABLE " + tb);
        }
        logger.info("Removed tables: " + Arrays.toString(tableNames));
    }

    private void createUseDB() {
        if (flagCreateDB) {
            if (flagDBExists) { // drop DB first
                spark.sql("DROP DATABASE " + dbName + " CASCADE");
                flagDBExists = !flagDBExists;
                logger.info("Dropped database: " + dbName);
            }
            if (!flagDBExists) { // create DB
                spark.sql("CREATE DATABASE IF NOT EXISTS " + dbName);
                flagDBExists = !flagDBExists;
                flagCreateDB = !flagCreateDB; // no need for this flag value anymore!
                logger.info("Created database: " + dbName);
            }
        }
        spark.sql("USE " + dbName);
        logger.info("Using the database: " + dbName);
    }
}

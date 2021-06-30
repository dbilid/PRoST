package loader2.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import run2.Main;

/**
 *
 * @author tioannid
 */
public class AsWKTDictionary implements Serializable {

    // ----- STATIC MEMBERS -----
    protected static final Logger logger = Logger.getLogger(Main.appName);

    // ----- STATIC METHODS -----
    // ----- DATA MEMEBERS -----
    private List<String> asWKTList;
    private final String asWKTTableName;
    private final boolean useHiveQL_TableCreation;
    private final SparkSession spark;
    private String hiveTableFormat;

    // ----- CONSTRUCTORS -----
    // 1. Detailed/Base Constructor
    /**
     * Read from the HDFS {@link asWKTFile} the asWKT properties and load them
     * to a list, while registering the name of the table where the list of
     * asWKT properties will be persisted and whether HiveQL should be used for
     * the creation.
     *
     * @param spark
     * @param asWKTFile HDFS file with the asWKT properties to use for indexing
     * @param asWKTTableName the table where the asWKT properties will be stored
     * @param useHiveQL_TableCreation <tt>true</tt> if the should be created
     * with HiveQL, otherwise create it with Spark SQL
     */
    public AsWKTDictionary(SparkSession spark, String asWKTFile,
            String asWKTTableName, boolean useHiveQL_TableCreation,
            String hiveTableFormat) {
        this.spark = spark;
        // read asWKT properties from asWKTFile HDFS file
        this.asWKTList = new ArrayList<>();
        if (!asWKTFile.isEmpty()) {
            asWKTList = spark.read().textFile(asWKTFile).collectAsList();
        }
        // TODO: Check if GEO.AS_WKT exists before inserting it again!
        // add the GeoSPARQL standard asWKT property
        asWKTList.add(GEO.AS_WKT.stringValue());
        // update properties for future Hive table creation
        this.asWKTTableName = asWKTTableName;
        this.useHiveQL_TableCreation = useHiveQL_TableCreation;
        this.hiveTableFormat = hiveTableFormat;
    }

    // 2. Constructor (with external file provided)
    public AsWKTDictionary(SparkSession spark, String asWKTFile,
            boolean useHiveQL_TableCreation, String hiveTableFormat) {
        this(spark, asWKTFile, "aswktprops", useHiveQL_TableCreation, hiveTableFormat);
    }

    // 3. Constructor (no external file provided), just the GeoSPARQL standard asWKT property
    public AsWKTDictionary(SparkSession spark, boolean useHiveQL_TableCreation,
            String hiveTableFormat) {
        this(spark, "", "aswktprops", useHiveQL_TableCreation, hiveTableFormat);
    }

    // ----- DATA ACCESSORS -----
    public List<String> getAsWKTList() {
        return asWKTList;
    }

    // ----- METHODS -----
    public void persist(List<String> inferredAsWKTList) {
        // update list
        this.asWKTList.addAll(inferredAsWKTList);
        // persist extra asWKT properties to Hive table
        Dataset<String> asWKTDS = spark.createDataset(asWKTList, Encoders.STRING()).distinct();
        // Create the aswktprops table and collect statistics 
        if (useHiveQL_TableCreation) { // use HiveQL
            asWKTDS.createOrReplaceTempView("tmp_asWKT");
            spark.sql(String.format(
                    "CREATE TABLE %1$s AS SELECT * FROM tmp_asWKT",
                    asWKTTableName));
        } else {    // use Spark SQL
            asWKTDS.write().format(hiveTableFormat).saveAsTable(asWKTTableName);
        }
        logger.info("ANALYZE TABLE " + asWKTTableName + " COMPUTE STATISTICS");
        spark.sql(String.format(
                "ANALYZE TABLE %1$s COMPUTE STATISTICS",
                asWKTTableName));
    }
}

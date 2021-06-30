package loader2;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import static loader2.Loader.logger;
import loader2.configuration.TripleTableSchema;
import loader2.utils.AsWKTDictionary;
import loader2.utils.Namespace;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import loader2.utils.NamespaceDictionary;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import scala.Tuple2;

/**
 * @author Theofilos Ioannidis
 */
public class TripleTableLoader extends Loader implements Serializable {

    // STATIC MEMBERS
    protected static String nsPrefTableName = "nsprefixes";

    // STATIC DATA ACCESSORS
    public static String getNsPrefTableName() {
        return nsPrefTableName;
    }

    // The use of this method is ONLY for setting a different name for
    // the table to store the namespace prefixes BEFORE creating a
    // new TripleTableLoader object.
    // TODO: It can be used to trigger the change of namespace prefixes table
    public static void setNsPrefTableName(String nsPrefTableName) {
        TripleTableLoader.nsPrefTableName = nsPrefTableName;
    }

    // DATA MEMBERS
    protected boolean ttPartitionedBySub = false;
    protected boolean ttPartitionedByPred = false;
    protected boolean dropDuplicates = true;
    // Schema of target table for thematic triples
    protected TripleTableSchema tttschema = null;
    // Schema of target table for geostatial triples
    protected TripleTableSchema gttschema = null;
    // protected final Broadcast<NamespaceDictionary> nsDict;
    protected boolean createUseNsDict = true;
    protected String namespacePrefixJSONFile = "";
    protected NamespaceDictionary nsDict = null;
    protected AsWKTDictionary asWKTDict = null;
    protected String hiveTableFormat;

    // CONSTRUCTORS
    // 1. Detailed/Base Constructor
    public TripleTableLoader(final SparkSession spark, final String dbName,
            boolean flagDBExists, boolean flagCreateDB,
            final String hdfsInputDir,
            boolean requiresInference,
            final boolean ttPartitionedBySub, final boolean ttPartitionedByPred,
            final boolean dropDuplicates,
            TripleTableSchema tttschema, TripleTableSchema gttschema,
            String namespacePrefixJSONFile, boolean createUseNsDict,
            final boolean useHiveQL_TableCreation,
            String asWKTFile,
            String hiveTableFormat) throws Exception {
        super(spark, dbName, flagDBExists, flagCreateDB, hdfsInputDir, useHiveQL_TableCreation);
        this.ttPartitionedBySub = ttPartitionedBySub;
        this.ttPartitionedByPred = ttPartitionedByPred;
        this.dropDuplicates = dropDuplicates;
        this.tttschema = tttschema;
        this.gttschema = gttschema;
        this.createUseNsDict = createUseNsDict;
        this.namespacePrefixJSONFile = namespacePrefixJSONFile;
        this.hiveTableFormat = hiveTableFormat;
        if (createUseNsDict) {
            nsDict = new NamespaceDictionary(spark, namespacePrefixJSONFile,
                    nsPrefTableName, useHiveQL_TableCreation, hiveTableFormat);
        } else {
            nsDict = null;
        }
        this.asWKTDict = new AsWKTDictionary(spark, asWKTFile,
                useHiveQL_TableCreation, hiveTableFormat);
    }

    // DATA ACCESSORS
    public boolean isTtPartitionedBySub() {
        return ttPartitionedBySub;
    }

    public void setTtPartitionedBySub(boolean ttPartitionedBySub) {
        this.ttPartitionedBySub = ttPartitionedBySub;
    }

    public boolean isTtPartitionedByPred() {
        return ttPartitionedByPred;
    }

    public void setTtPartitionedByPred(boolean ttPartitionedByPred) {
        this.ttPartitionedByPred = ttPartitionedByPred;
    }

    public boolean isDropDuplicates() {
        return dropDuplicates;
    }

    public void setDropDuplicates(boolean dropDuplicates) {
        this.dropDuplicates = dropDuplicates;
    }

    public TripleTableSchema getTttschema() {
        return tttschema;
    }

    public void setTttschema(TripleTableSchema tttschema) {
        this.tttschema = tttschema;
    }

    public TripleTableSchema getGttschema() {
        return gttschema;
    }

    public void setGttschema(TripleTableSchema gttschema) {
        this.gttschema = gttschema;
    }

    public boolean isCreateUseNsDict() {
        return createUseNsDict;
    }

    public void setCreateUseNsDict(boolean createUseNsDict) {
        this.createUseNsDict = createUseNsDict;
    }

    public String getNamespacePrefixJSONFile() {
        return namespacePrefixJSONFile;
    }

    public void setNamespacePrefixJSONFile(String namespacePrefixJSONFile) {
        this.namespacePrefixJSONFile = namespacePrefixJSONFile;
    }

    public NamespaceDictionary getNsDict() {
        return nsDict;
    }

    public void setNsDict(NamespaceDictionary nsDict) {
        this.nsDict = nsDict;
    }

    // Replace all occurences of ns.uri in rdf statement with ns.namespace
    // MAX 3 replacements are allowed per triple, which is the logical thing to expect!
    static class EncodeRDF implements Function<RDFStatement, RDFStatement> {

        private final List<Namespace> nsList;

        public EncodeRDF(List<Namespace> nsList) {
            this.nsList = nsList;
        }

        @Override
        public RDFStatement call(RDFStatement rdf) {
            Namespace ns;
            int cntReplaced = 0, listlen = nsList.size(), i;
            String serializedRDF = rdf.serialize(), serializedRDF_;
            //
            for (i = 0; (i < listlen && (cntReplaced != 3)); i++) {
                ns = nsList.get(i);
                serializedRDF_ = serializedRDF.replaceAll(ns.getUri(), ns.getNamespace());
                if (serializedRDF_.compareTo(serializedRDF) != 0) {
                    cntReplaced++;
                }
                serializedRDF = serializedRDF_;
            }
            return RDFStatement.deserialize(serializedRDF, rdf.getoType());
        }
    }

    // Selects the spatial indexed properties
    static class GetSpatialProps implements FlatMapFunction<RDFStatement, String> {

        private static final String AS_WKT = GEO.AS_WKT.stringValue();
        private static final String SUBPROPERTYOF = RDFS.SUBPROPERTYOF.stringValue();

        @Override
        public Iterator<String> call(RDFStatement rdf) {
            Set<String> s = new HashSet();
            String p = rdf.getP(), o = rdf.getO();
            if (p.equalsIgnoreCase(AS_WKT)) { // default GEO.AS_WKT is present
                s.add(p);
            } else if (p.equalsIgnoreCase(SUBPROPERTYOF)
                    && o.equalsIgnoreCase(AS_WKT)) { // subprops of GEO.AS_WKT are present
                s.add(rdf.getS());
            }
            return s.iterator();
        }
    }

    static class MarkThematicGeospatialRDF implements PairFunction<RDFStatement, RDFStatement, Integer> {

        public final List<String> lstAS_WKT;

        public MarkThematicGeospatialRDF(List<String> lstAS_WKT) {
            this.lstAS_WKT = lstAS_WKT;
        }

        @Override
        public Tuple2<RDFStatement, Integer> call(RDFStatement rdf) throws Exception {
            int type = 1; // 1=>thematic, 2=>spatial
            if (lstAS_WKT.contains(rdf.getP())) {
                type = 2;
            }
            return new Tuple2(rdf, type);
        }
    }

    static class FilterTriples implements FlatMapFunction<Tuple2<RDFStatement, Integer>, RDFStatement> {

        private final int type;

        public FilterTriples(int type) {
            this.type = type;
        }

        @Override
        public Iterator<RDFStatement> call(Tuple2<RDFStatement, Integer> t) throws Exception {
            Set<RDFStatement> s = new HashSet();
            if (t._2 == type) {
                s.add(t._1);
            }
            return s.iterator();
        }
    }

    // METHODS
    private void parseDirectory(String directory) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<String> inputFile, spatialPropsRDD;
        JavaRDD<RDFStatement> rdfRDD = null,
                spatialRDD = null, thematicRDD = null;
        // create an RDD of RDFStatement from all the parsed n-triple files
        ModularParser par = new ModularParser();
        Dataset<Row> triplesDF = null;
        try {
            RemoteIterator<LocatedFileStatus> i = FileSystem.get(sparkContext.hadoopConfiguration()).listLocatedStatus(new Path(directory));
            while (i.hasNext()) {
                Path path = i.next().getPath();
                triplesDF = spark.read().load(path.toString());
                inputFile = triplesDF.map(
                        (MapFunction<Row, String>) row -> row.getString(0),
                        Encoders.STRING()).javaRDD();
                if (rdfRDD != null) {
                    rdfRDD = rdfRDD.union(inputFile.map(line -> par.parseLine(line)));
                } else {
                    rdfRDD = inputFile.map(line -> par.parseLine(line));
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
//        logger.info("ALARM-DEBUG-0 : rdfRDD after parseDir has " + rdfRDD.count() + " records!");

        // get the list of geospatially indexed properties inferred from
        // n-triples files, update-merge in asWKTDict and persist it in Hive
        spatialPropsRDD = rdfRDD.flatMap(new GetSpatialProps()).distinct().cache();
        asWKTDict.persist(spatialPropsRDD.collect());

        // mark and then split RDFs to thematic and spatial
//        logger.info("ALARM-DEBUG-1 : rdfRDD before being mapped to pairs has " + rdfRDD.count() + " records!");
        JavaPairRDD<RDFStatement, Integer> markedRdfRDD = rdfRDD.mapToPair(new MarkThematicGeospatialRDF(asWKTDict.getAsWKTList()));
//        logger.info("ALARM-DEBUG-2 : markedRdfRDD has " + markedRdfRDD.count() + " records!");
        thematicRDD = markedRdfRDD.flatMap(new FilterTriples(1)); // Filters thematic triples
//        logger.info("ALARM-DEBUG-3 : thematicRDD has " + thematicRDD.count() + " records!");
        spatialRDD = markedRdfRDD.flatMap(new FilterTriples(2)); // Filters triples needed for spatial indexing
//        logger.info("ALARM-DEBUG-4 : spatialRDD has " + spatialRDD.count() + " records!");

        // if required, encode RDF statements
        if (createUseNsDict) {
            thematicRDD = thematicRDD.map(new EncodeRDF(nsDict.getNsList()));
            spatialRDD = spatialRDD.map(new EncodeRDF(nsDict.getNsList()));
        }
        // create Dataframes from RDDs
        Dataset<Row> dictEncodedRdfDS = spark.createDataFrame(thematicRDD, RDFStatement.class);
        Dataset<Row> dictEncodedSpatialRdfDS = spark.createDataFrame(spatialRDD, RDFStatement.class).select(col("s"), col("p"), col("o"));

        if (this.useHiveQL_TableCreation) { // use HiveQL
            dictEncodedRdfDS.createOrReplaceTempView("tmp_dictencoded");
            // Create the Thematic TriplesTable and collect statistics 
            spark.sql(String.format(
                    "CREATE TABLE %1$s AS SELECT * FROM tmp_dictencoded",
                    tttschema.getTblname()));
            spark.sql(String.format(
                    "ANALYZE TABLE %1$s COMPUTE STATISTICS",
                    tttschema.getTblname()));
            dictEncodedSpatialRdfDS.createOrReplaceTempView("tmp_dictencodedaswkt");
            if (!ttPartitionedByPred) {
                // Create the non-partitioned Spatial TriplesTable and collect statistics s, p 
                spark.sql(String.format(
                        "CREATE TABLE %1$s AS SELECT s,p,o, ST_GeomFromWKT(o) AS bwkt FROM tmp_dictencodedaswkt",
                        gttschema.getTblname()));
                spark.sql(String.format(
                        "ANALYZE TABLE %1$s COMPUTE STATISTICS FOR COLUMNS s,p,o",
                        gttschema.getTblname()));
            } else {
                // Create the partitioned Spatial TriplesTable and collect statistics s, p 
                spark.sql(String.format(
                        "CREATE TABLE %1$s USING HIVE PARTITIONED BY (p) AS SELECT s,o, ST_GeomFromWKT(o) AS bwkt, p FROM tmp_dictencodedaswkt",
                        gttschema.getTblname()));
                spark.sql(String.format(
                        "ANALYZE TABLE %1$s PARTITION(p) COMPUTE STATISTICS FOR COLUMNS s,o",
                        gttschema.getTblname()));
            }
        } else {    // use Spark SQL
            dictEncodedRdfDS.write().format(hiveTableFormat).saveAsTable(tttschema.getTblname());
            spark.sql(String.format(
                    "ANALYZE TABLE %1$s COMPUTE STATISTICS",
                    tttschema.getTblname()));
            if (!ttPartitionedByPred) {
                dictEncodedSpatialRdfDS.selectExpr("s", "p", "o", "ST_GeomFromWKT(o) AS bwkt").write().format(hiveTableFormat).saveAsTable(gttschema.getTblname());
                spark.sql(String.format(
                        "ANALYZE TABLE %1$s COMPUTE STATISTICS FOR COLUMNS s,p,o",
                        gttschema.getTblname()));
            } else {
                dictEncodedSpatialRdfDS.selectExpr("s", "p", "o", "ST_GeomFromWKT(o) AS bwkt").write().format(hiveTableFormat).partitionBy("p").saveAsTable(gttschema.getTblname());
                spark.sql(String.format(
                        "ANALYZE TABLE %1$s PARTITION(p) COMPUTE STATISTICS FOR COLUMNS s,o",
                        gttschema.getTblname()));
            }
        }
    }

    @Override
    public void load() throws Exception {
        logger.info("PHASE 1: loading all triples to a generic table...");
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tttschema.getTblname()));
//        logger.info(queryDropTripleTable);

        logger.info("Dropped " + tttschema.getTblname() + " table and recreating it");
        // Main line of code that performs parsing
        parseDirectory(hdfsInputDir);

////        spark.sql("show tables").show();
//        final String queryAllTriples = String.format("SELECT * FROM %s", tttschema.getTblname());
//        Dataset<Row> allTriples = spark.sql(queryAllTriples);
////        logger.info(queryAllTriples);
//
//        long noOfTriples = allTriples.count();
//        if (noOfTriples == 0) {
//            logger.error("Either your HDFS path does not contain any files or "
//                    + "no triples were accepted in the given format (nt)");
//            logger.error("The program will stop here.");
//            throw new Exception("Empty HDFS directory or empty files within.");
//        } else {
//            logger.info("Total number of triples loaded: " + noOfTriples);
//        }
//
//        final List<Row> cleanedList = allTriples.limit(10).collectAsList();
//        logger.info("First 10 cleaned triples (less if there are less): "
//                + ((nsDict != null) ? this.nsDict.getTurtlePrefixHeader() : "") + "\n" + cleanedList);
    }
}

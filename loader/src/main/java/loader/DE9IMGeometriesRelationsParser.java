package loader;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DE9IMGeometriesRelationsParser implements java.io.Serializable{
	private DE9IM relations;
	protected static final Logger logger = Logger.getLogger("PRoST");
	
	public DE9IM parseline(String line) throws Exception {
		try {
			String[] myData = line.replaceAll("IM\\(\\(", "").replaceAll("\\)","").replaceAll("<", "").replaceAll(">", "").split(",");
			/*need handling
			if(myData.length!=11) {
				
			}
			*/
			String id1 = 		myData[DE9IMEnum.id1.position()];
			String id2 = 		myData[DE9IMEnum.id2.position()];
			boolean Contains = 	Boolean.parseBoolean(myData[DE9IMEnum.Contains.position()]);
			boolean CoveredBy = Boolean.parseBoolean(myData[DE9IMEnum.CoveredBy.position()]);
			boolean Covers = 	Boolean.parseBoolean(myData[DE9IMEnum.Covers.position()]);
			boolean Crosses = 	Boolean.parseBoolean(myData[DE9IMEnum.Crosses.position()]);
			boolean Equals = 	Boolean.parseBoolean(myData[DE9IMEnum.Equals.position()]);
			boolean Intersects= Boolean.parseBoolean(myData[DE9IMEnum.Intersects.position()]);
			boolean Overlaps = 	Boolean.parseBoolean(myData[DE9IMEnum.Overlaps.position()]);
			boolean Touches = 	Boolean.parseBoolean(myData[DE9IMEnum.Touches.position()]);
			boolean Within = 	Boolean.parseBoolean(myData[DE9IMEnum.Within.position()]);
			this.relations = new DE9IM(id1, id2, Contains, CoveredBy, Covers, Crosses, Equals, Intersects, Overlaps, Touches, Within);		
		}
		catch(Exception e) {
            logger.error("Error while Parsing GeometriesRelations File:"+ e.getMessage());
		}
		return this.relations;	
	}

}

package org.apache.flink;


import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.CarbonContext;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CarbonDataOutputFormat extends RichOutputFormat<Row>{

	private static final Logger LOG = LoggerFactory.getLogger(CarbonDataOutputFormat.class);

	private String sparkMaster;
	private String storeLocation;
	private String appName = "flink-carbondata-default";
	private String queryTemplate;
	private CarbonContext carbonContext;
	private DataFrame dataFrame;
	int[] typeArray;
	public CarbonDataOutputFormat() {
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			SparkConf conf = new SparkConf().setMaster(sparkMaster).setAppName(appName);
			JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
			carbonContext = new CarbonContext(javaSparkContext.sc(), storeLocation);
		} catch(Exception ex){
			throw new IllegalArgumentException("open() failed." + ex.getMessage(), ex);
		}
	}

	@Override
	public void writeRecord(Row row) throws IOException {
		dataFrame = carbonContext.sql(queryTemplate);
	}

	@Override
	public void close() throws IOException {

	}

	public static CarbonDataOutputFormat.CarbonDataOutputFormatBuilder buildCarbonDataOutputFormat() {
		return new CarbonDataOutputFormatBuilder();
	}

	public static class CarbonDataOutputFormatBuilder {
		private final CarbonDataOutputFormat format;

		public CarbonDataOutputFormatBuilder() {

			this.format = new CarbonDataOutputFormat();
		}

		public CarbonDataOutputFormat.CarbonDataOutputFormatBuilder setMasterUrl(String sparkMaster) {
			format.sparkMaster = sparkMaster;
			return this;
		}

		public CarbonDataOutputFormat.CarbonDataOutputFormatBuilder setStoreLocation(String storeLocation) {
			format.storeLocation = storeLocation;
			return this;
		}

		public CarbonDataOutputFormat.CarbonDataOutputFormatBuilder setAppName(String appName) {
			format.appName = appName;
			return this;
		}

		public CarbonDataOutputFormat.CarbonDataOutputFormatBuilder setQuery(String query) {
			format.queryTemplate = query;
			return this;
		}

		public CarbonDataOutputFormat finish() {
			if (format.sparkMaster == null) {
				LOG.info("Spark Master URL was not supplied separately.");
			}
			if (format.storeLocation == null) {
				LOG.info("Store Location was not supplied separately");
			}
			return format;
		}

	}

}

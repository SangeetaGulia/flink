package org.apache.flink;

import java.io.IOException;
import java.util.Iterator;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.CarbonContext;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CarbonDataInputFormat<T>
    extends RichInputFormat<Row, InputSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(CarbonDataInputFormat.class);

  private String sparkMaster;
  private String storeLocation;
  private String appName = "flink-carbondata-default";
  private String queryTemplate;
  private CarbonContext carbonContext;
  private boolean hasNext;
  private DataFrame dataFrame;
  private Iterator<org.apache.spark.sql.Row> data;

  public CarbonDataInputFormat() {
  }

  @Override public void configure(Configuration parameters) {
    //do nothing here
  }

  @Override public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
      throws IOException {
    return cachedStatistics;
  }

  @Override public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
    return new InputSplit[0];
  }

  @Override public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override public void open(InputSplit split) throws IOException {
    try {
      dataFrame = carbonContext.sql(queryTemplate);
      data = dataFrame.collectAsList().iterator();
      hasNext = data.hasNext();
    } catch (Exception ex) {
      throw new IllegalArgumentException("open() failed." + ex.getMessage(), ex);
    }
  }

  @Override public boolean reachedEnd() throws IOException {
    return !hasNext;
  }

  @Override public Row nextRecord(Row row)
      throws IOException {
    try {
      if (!hasNext) {
        return null;
      }
      for (int pos = 0; pos < row.getArity(); pos++) {
        row.setField(pos, data.next().get(pos));
      }
      //update hasNext after we've read the record
      hasNext = data.next() != null;
      return row;

    } catch (NullPointerException npe) {
      throw new IOException("Couldn't access row", npe);
    } catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }

  @Override public void close() throws IOException {

  }

  @Override public void openInputFormat() {
    try {
      SparkConf conf = new SparkConf().setMaster(sparkMaster).setAppName(appName);
      JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
      carbonContext = new CarbonContext(javaSparkContext.sc(), storeLocation);
    } catch (Exception ex) {
      throw new IllegalArgumentException();
    }
  }

  @Override public void closeInputFormat() {
    try {
      carbonContext.sparkContext().cancelAllJobs();
      carbonContext.sparkContext().stop();
    } catch (Exception ex) {
      LOG.info("Inputformat couldn't be closed - " + ex.getMessage());
    } finally {
      carbonContext = null;
    }
  }

  public static CarbonDataInputFormatBuilder buildCarbonDataInputFormat() {
    return new CarbonDataInputFormatBuilder();
  }

  public static class CarbonDataInputFormatBuilder {
    private final CarbonDataInputFormat format;

    public CarbonDataInputFormatBuilder() {
      this.format = new CarbonDataInputFormat();
    }

    public CarbonDataInputFormatBuilder setMasterUrl(String sparkMaster) {
      format.sparkMaster = sparkMaster;
      return this;
    }

    public CarbonDataInputFormatBuilder setStoreLocation(String storeLocation) {
      format.storeLocation = storeLocation;
      return this;
    }

    public CarbonDataInputFormatBuilder setAppName(String appName) {
      format.appName = appName;
      return this;
    }

    public CarbonDataInputFormat finish() {
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

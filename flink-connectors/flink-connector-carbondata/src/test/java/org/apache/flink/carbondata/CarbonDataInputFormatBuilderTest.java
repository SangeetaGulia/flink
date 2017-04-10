package org.apache.flink.carbondata;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.junit.Test;

public class CarbonDataInputFormatBuilderTest {

  @Test
  public void testCarbonDataInOut() throws Exception{
    runTest();
  }

  private void runTest() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    CarbonDataInputFormat.CarbonDataInputFormatBuilder inputFormatBuilder = CarbonDataInputFormat
        .buildCarbonDataInputFormat()
        .setAppName("testApp")
        .setMasterUrl("spark://knoldus:7077")
        .setQuery("select * from interns")
        .setStoreLocation("hdfs://88.99.61.21:65110/tmp/perfsuite2");

    DataSource source = environment.createInput(inputFormatBuilder.finish());

    source.print();

  }

}

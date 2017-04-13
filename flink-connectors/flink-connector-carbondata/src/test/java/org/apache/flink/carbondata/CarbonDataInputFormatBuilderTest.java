package org.apache.flink.carbondata;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.CarbonContext;
import org.junit.Assert;
import org.junit.Test;

public class CarbonDataInputFormatBuilderTest {

  @Test
  public void testCarbonDataInOut() throws Exception{
    runTest();
  }

  private void runTest() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    CarbonContext carbonContext;
	  try {
		  SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("demo-app");
		  JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		  System.out.println("\n\n\n \n\n\n" + javaSparkContext.sc().sparkUser());
		  carbonContext = new CarbonContext(javaSparkContext.sc(), "hdfs://localhost:54310/opt/carbonStore");
		  System.out.println("\n\n\n\n\n\n" + carbonContext.storePath() + "\n\n\n\n\n\n");
	  } catch (Exception ex) {
		  ex.printStackTrace();
		  throw new IllegalArgumentException();
	  }
    CarbonDataInputFormat.CarbonDataInputFormatBuilder inputFormatBuilder = CarbonDataInputFormat
        .buildCarbonDataInputFormat()
        .setQuery("select * from interns")
		.setCarbonContext(carbonContext);


	  System.out.println("\n\n\n\n >>>>>>>>>>>>>>>>>>>>> Configured InputBuilder <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	  DataSource source = environment.createInput(inputFormatBuilder.finish());
	  System.out.println("\n\n\n\nCreated DataSource\n\n\n\n");
	  //Configuration inputDataConfig = new Configuration();

	  source.print();


  }

}

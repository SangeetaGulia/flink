package org.apache.flink;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.carbonutils.CarbonMultiBlockSplit;
import org.apache.flink.carbonutils.DictionaryDecodeReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

public class CarbonDataInputFormat<T> extends FileInputFormat<Void, T> {

	private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";
	private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
	private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
	private static final String FILTER_PREDICATE = "mapreduce.input.carboninputformat.filter.predicate";
	private static final Log LOG = LogFactory.getLog(CarbonDataInputFormat.class);


	@Override public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
															  TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		Configuration configuration = taskAttemptContext.getConfiguration();
		QueryModel queryModel = getQueryModel(inputSplit, taskAttemptContext);
		CarbonReadSupport<T> readSupport = getReadSupportClass(configuration);
		return new CarbonRecordReader<T>(queryModel, readSupport);
	}

	public QueryModel getQueryModel(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
		throws IOException {
		Configuration configuration = taskAttemptContext.getConfiguration();
		CarbonTable carbonTable = getCarbonTable(configuration);
		// getting the table absoluteTableIdentifier from the carbonTable
		// to avoid unnecessary deserialization
		AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();

		// query plan includes projection column
		String projection = getColumnProjection(configuration);
		CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(carbonTable, projection);
		QueryModel queryModel = QueryModel.createModel(identifier, queryPlan, carbonTable);

		// set the filter to the query model in order to filter blocklet before scan
		Expression filter = getFilterPredicates(configuration);
		CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);
		FilterResolverIntf filterIntf =  CarbonInputFormatUtil.resolveFilter(filter, identifier);
		queryModel.setFilterExpressionResolverTree(filterIntf);

		// update the file level index store if there are invalid segment
		if (inputSplit instanceof CarbonMultiBlockSplit) {
			CarbonMultiBlockSplit split = (CarbonMultiBlockSplit) inputSplit;
			List<String> invalidSegments = split.getAllSplits().get(0).getInvalidSegments();
			if (invalidSegments.size() > 0) {
				queryModel.setInvalidSegmentIds(invalidSegments);
			}
			List<UpdateVO> invalidTimestampRangeList =
				split.getAllSplits().get(0).getInvalidTimestampRange();
			if ((null != invalidTimestampRangeList) && (invalidTimestampRangeList.size() > 0)) {
				queryModel.setInvalidBlockForSegmentId(invalidTimestampRangeList);
			}
		}
		return queryModel;
	}

	public static CarbonTable getCarbonTable(Configuration configuration) throws IOException {
		String carbonTableStr = configuration.get(CARBON_TABLE);
		if (carbonTableStr == null) {
			populateCarbonTable(configuration);
			// read it from schema file in the store
			carbonTableStr = configuration.get(CARBON_TABLE);
			return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
		}
		return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
	}

	public CarbonReadSupport<T> getReadSupportClass(Configuration configuration) {
		String readSupportClass = configuration.get(CARBON_READ_SUPPORT);
		//By default it uses dictionary decoder read class
		CarbonReadSupport<T> readSupport = null;
		if (readSupportClass != null) {
			try {
				Class<?> myClass = Class.forName(readSupportClass);
				Constructor<?> constructor = myClass.getConstructors()[0];
				Object object = constructor.newInstance();
				if (object instanceof CarbonReadSupport) {
					readSupport = (CarbonReadSupport) object;
				}
			} catch (ClassNotFoundException ex) {
				LOG.error("Class " + readSupportClass + "not found", ex);
			} catch (Exception ex) {
				LOG.error("Error while creating " + readSupportClass, ex);
			}
		} else {
			readSupport = new DictionaryDecodeReadSupport<>();
		}
		return readSupport;
	}

	public static String getColumnProjection(Configuration configuration) {
		return configuration.get(COLUMN_PROJECTION);
	}

	private Expression getFilterPredicates(Configuration configuration) {
		try {
			String filterExprString = configuration.get(FILTER_PREDICATE);
			if (filterExprString == null) {
				return null;
			}
			Object filter = ObjectSerializationUtil.convertStringToObject(filterExprString);
			return (Expression) filter;
		} catch (IOException e) {
			throw new RuntimeException("Error while reading filter expression", e);
		}
	}

	/**
	 * this method will read the schema from the physical file and populate into CARBON_TABLE
	 * @param configuration
	 * @throws IOException
	 */
	private static void populateCarbonTable(Configuration configuration) throws IOException {
		String dirs = configuration.get(INPUT_DIR, "");
		String[] inputPaths = StringUtils.split(dirs);
		if (inputPaths.length == 0) {
			throw new InvalidPathException("No input paths specified in job");
		}
		AbsoluteTableIdentifier absoluteTableIdentifier =
			AbsoluteTableIdentifier.fromTablePath(inputPaths[0]);
		// read the schema file to get the absoluteTableIdentifier having the correct table id
		// persisted in the schema
		CarbonTable carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
		setCarbonTable(configuration, carbonTable);
	}

	/**
	 * It is optional, if user does not set then it reads from store
	 *
	 * @param configuration
	 * @param carbonTable
	 * @throws IOException
	 */
	public static void setCarbonTable(Configuration configuration, CarbonTable carbonTable)
		throws IOException {
		if (null != carbonTable) {
			configuration.set(CARBON_TABLE, ObjectSerializationUtil.convertObjectToString(carbonTable));
		}
	}
}

/*
 */
package com.adelya.csv2sql;

import io.deephaven.csv.CsvSpecs;
import static io.deephaven.csv.parsers.DataType.BOOLEAN_AS_BYTE;
import static io.deephaven.csv.parsers.DataType.BYTE;
import static io.deephaven.csv.parsers.DataType.CHAR;
import static io.deephaven.csv.parsers.DataType.INT;
import static io.deephaven.csv.parsers.DataType.SHORT;
import static io.deephaven.csv.parsers.DataType.STRING;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author mathi
 */
public class Csv2sql {
	
	StringBuilder createTable = new StringBuilder();
	List<String> proposedKeys = new ArrayList<>();
	String proposedPrimaryKey = "";
		
	public Csv2sql() {		
	}
	
	public void parse(String name, final InputStream inputStream)  throws Exception {
		
		init(name);
		
		final CsvSpecs specs = CsvSpecs.builder()
				.hasHeaderRow(true)				
				.delimiter(';')
				.build();
		final CsvReader.Result result = CsvReader.read(specs, inputStream, SinkFactory.arrays());
		
		final long numRows = result.numRows();
		System.out.format("found %d rows\n", numRows);
		int nbcol = 0;
		
		
		for (CsvReader.ResultColumn col : result) {
			
			System.out.format("col %s:%d is %s\n",col.name(), nbcol, col.dataType().toString());			
			
			switch (col.dataType()) {
				case BOOLEAN_AS_BYTE -> addColumn(col.name(),nbcol,"tinyint");
				case BYTE, SHORT -> addColumn(col.name(),nbcol, "SMALLINT");
				case INT -> addColumn(col.name(),nbcol, "INT");
				case LONG -> addColumn(col.name(),nbcol, "BIGINT");
				case FLOAT, DOUBLE -> addColumn(col.name(),nbcol, "double");
				case DATETIME_AS_LONG, TIMESTAMP_AS_LONG, CHAR -> addColumn(col.name(),nbcol,"varchar(1)");
				case STRING, CUSTOM -> addStringColumn(col,nbcol,"varchar(1)");				
			}			
			
			nbcol++;
		}
		
		close();
	}
	

	public static void main(String[] args) throws Exception {
				
		File file = new File("/adelya/TU/in/importer/importbig_members.csv");		
		
		Csv2sql csv2sql = new Csv2sql();
		csv2sql.parse("members", new FileInputStream(file));
	}
	

	private void addColumn(String name, int nbcol, String type) {
		checkKeys(name,nbcol);
		if (nbcol>0) createTable.append(",\n");
		createTable.append(" ").append(name).append(" ").append(type);	
	}

	private void init(String name) {
		createTable.append("create table ").append(name).append(" (");		
	}

	private void addStringColumn(CsvReader.ResultColumn col, int nbcol, String varchar1) {		
		checkKeys(name,nbcol);
		if (nbcol>0) createTable.append(",\n");
		createTable.append(" ").append(col.name()).append(" ").append(" varchar(255)");	
		/*
		byte[] data = (byte[]) col.data();
            // Process this short column.
           process(data, numRows);	
		   */
	}

	private void close() {
		createTable.append("\n )");
		if (StringUtils.isNotBlank(this.proposedPrimaryKey))
				createTable.append("-- PRIMARY KEY(")
				.append(this.proposedPrimaryKey).append(") \n");
		
		if (!this.proposedKeys.isEmpty()) {			
				createTable.append(this.proposedKeys.stream().collect(Collectors.joining(","))	);
		}
		 createTable.append(" ENGINE=InnoDB;");
		
		System.out.println(createTable.toString());
	}

	private void checkKeys(String name, int nbcol) {
		if (name.toLowerCase().contains("id")) {
			if (nbcol == 0) {
				this.proposedPrimaryKey = name;
			}
			else {
				this.proposedKeys.add(name);
			}
		}
	}
	
	
}

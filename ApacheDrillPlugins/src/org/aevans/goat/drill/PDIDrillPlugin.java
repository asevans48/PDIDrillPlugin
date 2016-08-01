package org.aevans.goat.drill;

import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.aevans.goat.drill.Query;
import org.apache.drill.jdbc.Driver;

/*
 * Author : Andrew Evans
 * Copyright 2016
 * License : Free BSD
 */

@DatabaseMetaPlugin(
		type = "DRILLJDBC", 
		typeDescription = "Apache Drill"
)

/**
 * This class contains the plugin using hte functions tested
 * in Query. 
 * 
 * @author aevans
 *
 */
public class PDIDrillPlugin extends BaseDatabaseMeta implements DatabaseInterface{
	String hostname = null;
	
	@Override
	/**
	 * This is Native JDBC only.
	 * @return		An integer array with the TYPE_ACCESS_NATIVE as the only element.
	 */
	public int[] getAccessTypeList(){
		return new int[]{DatabaseMeta.TYPE_ACCESS_NATIVE};
	}
	
	@Override
	/**
	 * No Port is required for the jdbc and many may actually be used.
	 * @return -1 for no Port.
	 */
	public int getDefaultDatabasePort(){
		return -1;
	}
	
	
	/**
	 * Returns the SQL query to execute when PDI needs to determine the field layout of a table
	 * @param		tableName		The table name should follow the schema.`table` format
	 */
	@Override
	public String getSQLQueryFields(String tableName) {
		return "SELECT * FROM " + tableName;
	}
	
	/**
	 * Select only a column name to see if it exists. Drill JDBC supports meta data
	 * @param columnname
	 * @param tablename		Should be schema.`table`
	 * @return The Select statement
	 */
	public String getColumnExists(String columnname, String tablename){
		return "SELECT "+columnname+" FROM "+tablename+" LIMIT 1";
	}

	
	/**
	 * Drill supports options in its url including cluster ids and multiple
	 * zookeeper hosts.
	 * @return true always
	 */
	@Override
	public boolean supportsOptionsInURL(){
		return true;
	}

	@Override
	/**
	 *Drill cab create Tables but it is more recommended to 
	 *dump to a file with CSV or Json or via the SQL statement command 
	 *because a SQL statement creates a file but the file is multi-part
	 *and if it is not Json will be non-recombinable without more processing.
	 *@return An empty string
	 */
	public String getAddColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
		return "";
	}
	
	@Override
	public String getDriverClass() {
		return Driver.class.getName();
	}

	
	/**
	 * @See getAddColumnStatement
	 */
	@Override
	public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc, boolean add_fieldname, boolean add_cr) {
		return "";
	}

	
	/**
	 * @See getAddColumnStatement
	 */
	@Override
	public String getModifyColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
		return "";
	}
	
	
	/**
	 * Get the connection String which should specify zookeeper host with port +  cluster id if applicable and 
	 * the schema as the databaseName. This manipulates the Pentaho connection idea only slightly.
	 * 
	 * @param		hostString		The zookeeper hosts string
	 * @param		clusterId 		The cluster id as applicable
	 * @param		schema			The schema to use (required to avoid issues)
	 * @return		The full connection String
	 */
	@Override
	public String getURL(String hostString, String clusterId, String schema) throws KettleDatabaseException {
		if(clusterId != null && clusterId.trim().length() >0 && !clusterId.equals("-1")){
			this.hostname = "jdbc:drill:zk="+hostString+clusterId+";schema="+schema;
			return "jdbc:drill:zk="+hostString+clusterId+";schema="+schema;
		}
		this.hostname = "jdbc:drill:zk="+hostString+";"+"schema="+schema;
		return "jdbc:drill:zk="+hostString+";"+"schema="+schema;
	}

	@Override
	public String[] getUsedLibraries() {
		return new String[]{"drill-jdbc-all-1.6.0.jar"};
	}
	
	
	/**
	 * Must obtain actual query to build table
	 */
	@Override
	public boolean supportsPreparedStatementMetadataRetrieval() {
		return false;
	}
	
	
	/**
	 * Must look at result set for table data
	 */
	@Override
	public boolean supportsResultSetMetadataRetrievalOnly() {
		return true;
	}
	
	
	/**
	 * Drill has full savepoint except for releaseSavepoint
	 */
	@Override
	public boolean releaseSavepoint(){
		return false;
	}
	
	
	/**
	 * No transactions
	 */
	@Override
	public boolean supportsTransactions(){
		return false;
	}
}

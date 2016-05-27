package com.hygenics.drill;


import java.sql.*;
import org.apache.drill.jdbc.Driver;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

/**
 * Try to avoid thousands of extra queries.
 * @author aevans
 *
 */
public class Query {
	private String zkURL;
	private String drillDriver = "org.apache.drill.jdbc.Driver";
	private List<String> headers;
	private boolean debug = false;
	
	public void setDebug(boolean debug){
		this.debug = debug;
	}
	
	public List<String> getHeaders(){
		return headers;
	}
	
	public void clearHeaders(){
		headers = new ArrayList<String>();
	}
	
	public String getZkURL() {
		return zkURL;
	}



	public void setZkURL(String zkURL) {
		this.zkURL = zkURL;
	}



	public String getDrillDriver() {
		return drillDriver;
	}



	public void setDrillDriver(String drillDriver) {
		this.drillDriver = drillDriver;
	}



	public List<Map<String,Object>> query(String sql) throws SQLException{
		ArrayList<Map<String, Object>> results = new ArrayList<Map<String,Object>>();
		Connection con = null;
		try{
			con = new Driver().connect(this.zkURL, System.getProperties());
			Statement stmt= con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("Getting Data");
			ResultSetMetaData md  = rs.getMetaData();
			System.out.println("|");
			headers = new ArrayList<String>();
			for(int i = 1 ; i < md.getColumnCount();i++){
					String colName = md.getColumnName(i);
					if(debug){
						System.out.print(colName+"|");
					}
					headers.add(colName);
			}
			if(debug){
				System.out.print("\n");
			}
				
				
			while(rs.next()){
					if(debug){
						System.out.println("--------------");
					}
					Map<String,Object> record = new HashMap<String,Object>();
					for(int i = 1 ;i < md.getColumnCount();i++){
						Object o = rs.getObject(i);
						if(debug){
							if(o!= null){
								System.out.print(o.toString()+"|");
							}else{
								System.out.print("null|");
							}
							
						}
						record.put(md.getTableName(i), o);
					}
					if(debug){
						System.out.print("\n");
					}
					results.add(record);
			}
			
		}catch (Exception e){
			e.printStackTrace();
		}finally{
			if(con != null && !con.isClosed()){
				con.close();
			}
		}
		
		return results;
	}
	
	public void execute(String sql) throws SQLException{
		Connection con = null;
	
		try{
			con = new Driver().connect(this.zkURL, System.getProperties());
			PreparedStatement ps = con.prepareStatement(sql);
			ps.execute();
		}catch (Exception e){
			e.printStackTrace();
		}finally{
			if(con != null && !con.isClosed()){
				con.close();
			}
		}
	}
	
	public ResultSetMetaData getMetaData(String sql) throws SQLException{
		Connection con = null;
		ResultSetMetaData rsm = null;
		try{
			con = new Driver().connect(this.zkURL, System.getProperties());
			PreparedStatement ps  = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			rsm = rs.getMetaData();
		}catch (Exception e){
			e.printStackTrace();
		}finally{
			if(con != null && !con.isClosed()){
				con.close();
			}
		}
		return rsm;
	}
	
	public static void main(String[] args){
		Query q = new Query();
		q.setZkURL("jdbc:drill:zk=localhost;schema=fs");
		q.setDrillDriver("org.apache.drill.jdbc.Driver");
		try{
			List<Map<String,Object>> r = q.query("SELECT * FROM fs.`/home/aevans/drilldata/mt_sor_records.json`");
			List<String> headers = q.getHeaders();
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
}
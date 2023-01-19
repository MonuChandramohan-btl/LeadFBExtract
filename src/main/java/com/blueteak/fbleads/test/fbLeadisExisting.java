package com.blueteak.fbleads.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.blueteak.csvutil.CSVHelper;
import com.blueteak.csvutil.CSVReader;
import com.blueteak.fbleads.constants.FbExtractConstants;
import com.blueteak.request.FBLeadRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

public class fbLeadisExisting {

	private static String fbFileLeadName = "FbLeadsCreatedDateUpdated";
	static final String DB_URL = "jdbc:sqlserver://15.0.4.4:1433;databaseName=dms-leads";
	static final String USER = "sa";
	static final String PASS = "h32YP9cw4N2VJrX8sd";
	public static String fbSelectQuery = "select CUST_NAME ,EMAIL ,NOTES ,PHONE,SRC from T_LEAD "
			+ "where "
			+ "CUST_NAME =  '%1$s' "
			+ "and EMAIL  =  '%2$s' and "
			+ " CONVERT(VARCHAR(MAX), NOTES) = '%3$s' and PHONE = '%4$s' "
			+ "and  src='%5$s' ";
			//+ "and CREATED_DATE_TIME >= '%6$s';

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {

		File files = new File("src\\main\\resources\\combined");
		CSVHelper csvHelper = new CSVHelper();
		
		CSVReader csvReader = new CSVReader(csvHelper);
		List<FBLeadRequest> fbLeadReqList = new ArrayList<FBLeadRequest>();
		for (File file : files.listFiles()) {
			if (file.isFile()) {
				fbLeadReqList.addAll(csvReader.ReadFromCSV(file.getName()));
				System.out
						.println("=================CSV READ SUCCESSFULL :: " + file.getName() + "===================");
				System.out.println("##reqCount## " + fbLeadReqList.size());
			}
		}

		HashMap<String, FBLeadRequest> fbLeadMap = new HashMap<String, FBLeadRequest>();
		for (FBLeadRequest fbLeadRequest : fbLeadReqList) {
			fbLeadMap.put(fbLeadRequest.getLeadId(), fbLeadRequest);
		}

		isProcessed(fbLeadMap);

	}

	public static void isProcessed(HashMap<String, FBLeadRequest> fbLeadMap) throws IOException {

		Connection connection = null;
		Statement selectStmt = null;
		File myObj = getFile(fbFileLeadName);
		FileWriter fileWriter = new FileWriter(myObj);
		ObjectMapper Obj = new ObjectMapper();

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			connection = DriverManager.getConnection(DB_URL, USER, PASS);
			int count = 0;
			int notPresent = 0;
			int notMigrated =0;
			for (String leadId : FbExtractConstants.fbLeadIds) {
				System.out.println("Count :: " + count++);
				if (fbLeadMap.containsKey(leadId)) {
					FBLeadRequest fbLeadReq = fbLeadMap.get(leadId);
					String selectQuery = String.format(fbSelectQuery, 
							fbLeadReq.getCustomerFullName(),
							fbLeadReq.getEmail(), 
							fbLeadReq.getModel(), 
							fbLeadReq.getPhoneNumber(), 
							"FB");
							//"2023-01-17");
					selectStmt = connection.createStatement();
					ResultSet rs = selectStmt.executeQuery(selectQuery);
					boolean isPresent = false;
					while (rs.next()) {
						if (rs.getString(1).equalsIgnoreCase(fbLeadReq.getCustomerFullName())
								&& rs.getString(2).equalsIgnoreCase(fbLeadReq.getEmail())
								&& rs.getString(3).equalsIgnoreCase(fbLeadReq.getModel())
								&& rs.getString(4).equalsIgnoreCase(fbLeadReq.getPhoneNumber())
								&& rs.getString(5).equalsIgnoreCase("FB")) {
							isPresent = true;
							break;
						}
					}
					if (isPresent) {
						//System.out.println(count + ": "+Obj.writeValueAsString(fbLeadReq) + "  \n");
						//fileWriter.write(count + ": "+Obj.writeValueAsString(fbLeadReq) + "  \n");
					} else {
						notMigrated++;
						System.out.println(selectQuery);
						System.out.println(notMigrated + ": "+"Not migrated :" + leadId +" :: "+ Obj.writeValueAsString(fbLeadReq)+" \n");
						fileWriter.write(notMigrated + ": "+"Not migrated :" + leadId +" :: "+ Obj.writeValueAsString(fbLeadReq)+" \n");
					}
				}
//				else {
//					System.out.println(count + ": notpresent :"+ notPresent++ + " : Lead id " + leadId + " is not present in the files. \n");
//					fileWriter.write(count + ": notpresent :"+ notPresent++ + " : Lead id " + leadId + " is not present in the files. \n");
//				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				selectStmt.close();
				connection.close();
				fileWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void processResponse(HashMap<String, FBLeadRequest> fbLeadMap) throws IOException {
		File myObj = getFile(fbFileLeadName);
		FileWriter fileWriter = new FileWriter(myObj);
		ObjectMapper Obj = new ObjectMapper();
		try {
			int count = 0;
			int notPresent = 0;
			for (String leadId : FbExtractConstants.fbLeadIds) {
				System.out.println("Count :: " + count++);
				if (fbLeadMap.containsKey(leadId)) {
					FBLeadRequest fbLeadReq = fbLeadMap.get(leadId);
					String updateQuery = String.format(FbExtractConstants.fbUpdateQuery, fbLeadReq.getCreatedDateTime(),
							fbLeadReq.getCustomerFullName(), fbLeadReq.getEmail(), fbLeadReq.getModel(),
							fbLeadReq.getPhoneNumber());
					fileWriter.write(updateQuery + "  \n");
				} else {
					fileWriter.write("--" + notPresent++ + " : Lead id " + leadId + " is not present in the files. \n");
				}
			}
		} finally {
			fileWriter.close();
		}
	}

	private static File getFile(String fileName) {
		String path = "src/main/resources/combined";
		File myObj = null;
		try {
			myObj = new File(path + "/" + fileName + ".txt");
			if (myObj.createNewFile()) {
				System.out.println("File created: " + myObj.getName());
			} else {
				System.out.println("File already exists.");
			}
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		return myObj;
	}

}

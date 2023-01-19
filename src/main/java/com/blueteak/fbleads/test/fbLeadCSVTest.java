package com.blueteak.fbleads.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import javax.swing.text.html.parser.Entity;

import com.blueteak.csvutil.CSVHelper;
import com.blueteak.csvutil.CSVReader;
import com.blueteak.csvutil.CSVWriter;
import com.blueteak.fbleads.constants.FbExtractConstants;
import com.blueteak.request.FBLeadRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class fbLeadCSVTest {

	private static String fbFileLeadName = "FbLeadsNewAfterDec28ANDBefjan16";
	static final String DB_URL = "jdbc:sqlserver://15.0.4.4:1433;databaseName=dms-leads";
	static final String USER = "sa";
	static final String PASS = "h32YP9cw4N2VJrX8sd";
	public static String fbSelectQuery = "select CUST_NAME ,EMAIL ,NOTES ,PHONE,SRC from T_LEAD " + "where "
			+ "CREATED_DATE_TIME >= '%1$s' and " + "CUST_NAME =  '%2$s' "
			+ "and EMAIL  =  '%3$s' and CONVERT(VARCHAR(MAX), NOTES) = '%4$s' and PHONE = '%5$s' and  src='%6$s' ";
	
	public static void main(String[] args) {
		String selectQuery = String.format(fbSelectQuery, "2023-01-18", "custName",
				"email", "model", "phoneno", "FB");
		System.out.println(selectQuery);
	}

	public static void main1(String[] args) throws FileNotFoundException, IOException, ParseException {

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
		Set<String> set = readByLine();
		System.out.println("Size of set :: " + set.size());
		HashMap<String, FBLeadRequest> fbLeadMap = new HashMap<String, FBLeadRequest>();
		for (FBLeadRequest fbLeadRequest : fbLeadReqList) {
			fbLeadMap.put(fbLeadRequest.getLeadId(), fbLeadRequest);
		}
		List<FBLeadRequest> fbLeadReq = processResponse(fbLeadMap, set);

		CSVWriter csvWriter = new CSVWriter("combined.csv", fbLeadReq, csvHelper, csvReader);
		csvWriter.writeCSV();
	}

	private static Set<String> readByLine() throws ParseException {
		Set<String> set = new HashSet();
		BufferedReader reader;
		File files = new File("src\\main\\resources\\ExistingLeads");
		for (File file : files.listFiles()) {
			if (file.isFile()) {
				try {
					reader = new BufferedReader(new FileReader(file));
					String line = reader.readLine();
					while (line != null) {
						ObjectMapper objMapper = new ObjectMapper();
						try {
							FBLeadRequest fbLeadRequest = objMapper.readValue(line, FBLeadRequest.class);
							fbLeadRequest.setPhoneNumber(fbLeadRequest.getPhoneNumber().replace("+60", "+60-"));
							String convertedDate = convertDate(fbLeadRequest.getCreatedDateTime());
							fbLeadRequest.setCreatedDateTime(convertedDate);
							fbLeadRequest.setCustomerFullName(fbLeadRequest.getCustomerFullName().replace("'", "''"));
							fbLeadRequest.setModel(fbLeadRequest.getModel().replace("'", "''"));
							String LeadId = fbLeadRequest.getLeadId();
							fbLeadRequest.setLeadId(LeadId.substring(LeadId.indexOf(":") + 1));
							String result = objMapper.writeValueAsString(fbLeadRequest);
							set.add(result);
							System.out.println(result);
						} catch (Exception e) {
							System.out.println("Skipped .." + line);
						}
						line = reader.readLine();
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return set;
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
			for (String leadId : FbExtractConstants.fbLeadIds) {
				if (fbLeadMap.containsKey(leadId)) {
					FBLeadRequest fbLeadReq = fbLeadMap.get(leadId);
					String selectQuery = String.format(fbSelectQuery, "2023-01-18", fbLeadReq.getCustomerFullName(),
							fbLeadReq.getEmail(), fbLeadReq.getModel(), fbLeadReq.getPhoneNumber(), "FB");
					selectStmt = connection.createStatement();
					ResultSet rs = selectStmt.executeQuery(selectQuery);
					boolean isPresent = false;
					while (rs.next()) {
						System.out.println("Count :: " + count++);
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
						fileWriter.write(count + ": " + Obj.writeValueAsString(fbLeadReq) + "  \n");
					} else {
						fileWriter.write(
								count + ": " + "Migrated Lead id " + leadId + " is not present in the files. \n");
					}
				} else {
					fileWriter.write(
							count + ": " + notPresent++ + " : Lead id " + leadId + " is not present in the files. \n");
				}
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

		}
	}

	public static List<FBLeadRequest> processResponse(HashMap<String, FBLeadRequest> fbLeadMap, Set<String> set) throws IOException, ParseException {
		List<FBLeadRequest> result = new ArrayList();
		File myObj = getFile(fbFileLeadName);
		FileWriter fileWriter = new FileWriter(myObj);
		ObjectMapper mapper = new ObjectMapper();
		try {
			int count = 0;
			for (Entry<String, FBLeadRequest> entry : fbLeadMap.entrySet()) {
				String json = mapper.writeValueAsString(entry.getValue());
				FBLeadRequest fbLeadRequest = entry.getValue();
				System.out.println(fbLeadRequest.toString());
				Date createdDate = getDate(fbLeadRequest.getCreatedDateTime());
				Date afterDate = genAfterDate();
				Date beforeDate = genBeforeDate();
				if(createdDate.after(afterDate) && createdDate.before(beforeDate)) {
					result.add(entry.getValue());
					System.out.println("Count :: " + count++);
					
					
					ArrayNode array = mapper.createArrayNode();
					
					ObjectNode phoneNumber = mapper.createObjectNode();
					phoneNumber.put("name", "Phone Number");
					phoneNumber.put("value", fbLeadRequest.getPhoneNumber());
					array.add(phoneNumber);
					
					ObjectNode conditional_question_2 = mapper.createObjectNode();
					conditional_question_2.put("name", "conditional_question_2");
					conditional_question_2.put("value", fbLeadRequest.getQuestion2());
					array.add(conditional_question_2);
					
					ObjectNode email = mapper.createObjectNode();
					email.put("name", "Email");
					email.put("value", fbLeadRequest.getEmail());
					array.add(email);
					
					ObjectNode fullName = mapper.createObjectNode();
					fullName.put("name", "Full Name");
					fullName.put("value", fbLeadRequest.getCustomerFullName());
					array.add(fullName);
					
					ObjectNode conditional_question_1 = mapper.createObjectNode();
					conditional_question_1.put("name", "conditional_question_1");
					conditional_question_1.put("value", fbLeadRequest.getQuestion1());
					array.add(conditional_question_1);
					
					ObjectNode model = mapper.createObjectNode();
					model.put("name", "which_model_are_you_interested_in");
					model.put("value", fbLeadRequest.getModel());
					array.add(model);
					
					
					ObjectNode event = mapper.createObjectNode();
					event.put("event", array);
					
					String jsonString = mapper.writeValueAsString(event);
					System.out.println(jsonString);
					fileWriter.write(count + ": " + jsonString + "  \n");
					
				}
			}
		} finally {
			fileWriter.close();
		}
		return result;
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

	private static String convertDate(String date) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		Date result = df.parse(date);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
		sdf.setTimeZone(TimeZone.getTimeZone("Singapore"));
		String convertedDate = sdf.format(result);
		return convertedDate;
	}

	private static Date getDate(String date) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
		Date result = df.parse(date);
		return result;
	}

	private static Date genAfterDate() {
		String dateInString = "29-Dec-2022";
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		Date afterDate = null;
		try {
		    afterDate = formatter.parse(dateInString);
		    System.out.println(afterDate);
		} catch (ParseException e) {
		    //handle exception if date is not in "dd-MMM-yyyy" format
		}
		return afterDate;
	}
	
	private static Date genBeforeDate() {
		String dateInString = "16-Jan-2023";
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		Date afterDate = null;
		try {
		    afterDate = formatter.parse(dateInString);
		    System.out.println(afterDate);
		} catch (ParseException e) {
		    //handle exception if date is not in "dd-MMM-yyyy" format
		}
		return afterDate;
	}
	
	
}

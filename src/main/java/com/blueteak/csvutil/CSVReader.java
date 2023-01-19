package com.blueteak.csvutil;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import com.blueteak.fbleads.constants.FbExtractConstants;
import com.blueteak.request.FBLeadRequest;

public class CSVReader {

	private CSVHelper csvHelper;

	public CSVReader(CSVHelper csvHelper) {
		this.csvHelper = csvHelper;
	}

	public static void main(String[] args) throws IOException, ParseException {
		CSVReader csvReader = new CSVReader(new CSVHelper());
		csvReader.ReadFromCSV(FbExtractConstants.FB_LEAD_FILE_NAME);
	}

	/**
	 * @param readCsvFilename
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws ParseException
	 */
	public List<FBLeadRequest> ReadFromCSV(String readCsvFilename)
			throws IOException, FileNotFoundException, ParseException {
		try (ICsvMapReader listReader = new CsvMapReader(new FileReader(csvHelper.getAbsPath() + "/" + readCsvFilename),
				CsvPreference.STANDARD_PREFERENCE)) {
			final String[] headers = listReader.getHeader(true);
			List<FBLeadRequest> fbLeadReqList = new ArrayList<>();
			Map<String, String> fieldsInCurrentRow;
			FBLeadRequest fbLeadRequest;
			while ((fieldsInCurrentRow = listReader.read(headers)) != null) {
				fbLeadRequest = setLeadFromCsv(fieldsInCurrentRow);
				// System.out.println(jsonStr);
				fbLeadReqList.add(fbLeadRequest);
			}
			return fbLeadReqList;
		}
	}

	/**
	 * @param fieldsInCurrentRow
	 * @return
	 * @throws ParseException
	 */
	private FBLeadRequest setLeadFromCsv(Map<String, String> fieldsInCurrentRow) throws ParseException {
		FBLeadRequest fbLeadRequest = new FBLeadRequest();
		for (Entry<String, String> entry : fieldsInCurrentRow.entrySet()) {
			// System.out.print(" :: "+entry.getKey()+ " : "+entry.getValue());
			switch (entry.getKey()) {
			case "Full Name":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					fbLeadRequest.setCustomerFullName(entry.getValue().replace("'","''"));
				}
				break;
			case "Email":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					fbLeadRequest.setEmail(entry.getValue());
				}
				break;
			case "form_name":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					String s = entry.getValue().replace("'","''");
					fbLeadRequest.setModel(s.substring(s.indexOf(":") + 1));
				}
				break;
			case "Phone Number":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					String s = entry.getValue().replace("+60","+60-");
					fbLeadRequest.setPhoneNumber(s.substring(s.indexOf(":") + 1));
				}
				break;
			case "conditional_question_1":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					fbLeadRequest.setQuestion1(entry.getValue());
				}
				break;
			case "conditional_question_2":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					fbLeadRequest.setQuestion2(entry.getValue());
				}
				break;
			case "﻿id":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					String s = entry.getValue();
					fbLeadRequest.setLeadId(s.substring(s.indexOf(":") + 1));
				}
				break;
			case "created_time":
				if (entry.getValue() != null && !StringUtils.equalsIgnoreCase("NULL", String.valueOf(entry.getValue()))
						&& StringUtils.isNotBlank(String.valueOf(entry.getValue()))) {
					String convertedDate = convertDate(entry);
					fbLeadRequest.setCreatedDateTime(convertedDate);
				}
				break;
			default:
				break;
			}
		}
		// System.out.println();
		return fbLeadRequest;
	}

	private String convertDate(Entry<String, String> entry) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		Date result = df.parse(entry.getValue());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
		sdf.setTimeZone(TimeZone.getTimeZone("Singapore"));
		String convertedDate = sdf.format(result);
		return convertedDate;
	}

}

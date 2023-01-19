package com.blueteak.csvutil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import com.blueteak.request.FBLeadRequest;

public class CSVWriter implements Runnable {


	static volatile boolean isHeadedAdded = false;
	private final Object lock;
	private String writeCsvFilename = null;
	private List<FBLeadRequest> fbLeadReqList = new ArrayList<>();
	private CSVHelper csvHelper;

	public CSVWriter(String writeCsvFilename, List<FBLeadRequest> fbLeadReqList, CSVHelper csvHelper,
			Object lock2) {
		this.lock = lock2;
		this.writeCsvFilename = writeCsvFilename;
		this.fbLeadReqList = fbLeadReqList;
		this.csvHelper = csvHelper;
	}
	

	@Override
	public void run() {
		synchronized (lock) {
			this.writeCSV();
		}
	}

	/**
	 * @param writeCsvFilename
	 * @param leadResList
	 * 
	 */
	public void writeCSV() {
		System.out.println("Started writing csv..");
		ICsvBeanWriter beanWriter = null;
		try {
			beanWriter = new CsvBeanWriter(new FileWriter(csvHelper.getAbsPath() +"/Recovered_"+  writeCsvFilename, isHeadedAdded),
					CsvPreference.STANDARD_PREFERENCE);
			final String[] header = new String[] {  "LEADID", "CREATEDDATETIME", "MODEL", "QUESTION1",
					"QUESTION2", "CUSTOMERFULLNAME" ,"EMAIL","PHONENUMBER"};
			final String[] mapping = new String[] { "leadId", "createdDateTime", "model", "question1",
					"question2", "customerFullName" ,"email","phoneNumber"};

			if (!isHeadedAdded) {
				beanWriter.writeHeader(header);
				isHeadedAdded = true;
			}

			// write the beans data
			for (FBLeadRequest c : fbLeadReqList) {
				beanWriter.write(c, mapping);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				beanWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("writing csv completed..");
		}
	}

}

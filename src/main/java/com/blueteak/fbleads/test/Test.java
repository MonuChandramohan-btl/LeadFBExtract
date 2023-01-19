package com.blueteak.fbleads.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Test {
	
	public static void main(String[] args) throws ParseException {
		//"2023-01-17 12:02:53.210"
		//"2023-01-08 22:14:31.031"
		
		String dateToBeConverted ="2023-01-08T22:14:31+08:00";
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		Date result;
		String finalRes = null;
		try {
		    result = df.parse(dateToBeConverted);
		    System.out.println("date:"+result); //prints date in current locale
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
		    sdf.setTimeZone(TimeZone.getTimeZone("Singapore"));
		    finalRes = sdf.format(result);
		    System.out.println(sdf.format(result)); //prints date in the format sdf
		}finally {
			System.out.println("date Converted successfully.");
		}
		
		String updateQuery = "update T_LEAD set CREATED_DATE_TIME =  '%1$s' where CREATED_DATE_TIME >= cast(getdate() as date) and CUST_NAME =  '%2$s' and EMAIL  =  '%3$s' and CONVERT(VARCHAR(MAX), NOTES) = '%4$s' and PHONE = '%5$s' ";
		String fstr = String.format(updateQuery, finalRes, "Madung", "madung.marupe@genting.com", "FORTUNER", "+60-138116459");
		System.out.println(fstr);
	}

}

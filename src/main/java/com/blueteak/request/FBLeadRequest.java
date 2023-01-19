package com.blueteak.request;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

//"customerFullName": "Kay Camelia",
//"email": "kay_camelia81@yahoo.com",
//"model": "COROLLA CROSS HYBRID",
//"phoneNumber": "+60173631781",
//"question1": "Negeri",
//"question2": "Toyota Nilai (Unitedstar Corporation)"
//
//	
@Getter
@Setter
@ToString
public class FBLeadRequest {

	private String leadId;
	private String createdDateTime;
	private String model;
	private String question1; 
	private String question2; 
	private String customerFullName;
	private String email;
	private String phoneNumber;

}

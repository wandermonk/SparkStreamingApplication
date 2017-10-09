package com.api.streams.domains;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("person")
public class Person implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String name;
	private String personalID;
	private String country;
	private String occupation;

	public Person() {

	}
	
	@JsonCreator
	public Person(@JsonProperty("name") String name,@JsonProperty("personalID") String personalID,
			@JsonProperty("country") String country,@JsonProperty("occupation") String occupation){
		this.name= name;
		this.personalID = personalID;
		this.country = country;
		this.occupation = occupation;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPersonalID() {
		return personalID;
	}

	public void setPersonalID(String personalID) {
		this.personalID = personalID;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getOccupation() {
		return occupation;
	}

	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}

	@Override
	public String toString() {
		return "{" + " " + "Name :" + " " + name + " " + "ID :" + " "
				+ personalID + " " + "Country :" + " " + country + " "
				+ "Occupation :" + " " + occupation + "}";
	}
}

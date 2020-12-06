
package com.raj.nola.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "streetAddress",
    "city",
    "state",
    "country",
    "zipCode",
    "contactNumber"
})
public class Address {

    @JsonProperty("streetAddress")
    private String streetAddress;
    @JsonProperty("city")
    private String city;
    @JsonProperty("state")
    private String state;
    @JsonProperty("country")
    private String country;
    @JsonProperty("zipCode")
    private Integer zipCode;
    @JsonProperty("contactNumber")
    private Object contactNumber;

    @JsonProperty("streetAddress")
    public String getStreetAddress() {
        return streetAddress;
    }

    @JsonProperty("streetAddress")
    public void setStreetAddress(String streetAddress) {
        this.streetAddress = streetAddress;
    }

    public Address withStreetAddress(String streetAddress) {
        this.streetAddress = streetAddress;
        return this;
    }

    @JsonProperty("city")
    public String getCity() {
        return city;
    }

    @JsonProperty("city")
    public void setCity(String city) {
        this.city = city;
    }

    public Address withCity(String city) {
        this.city = city;
        return this;
    }

    @JsonProperty("state")
    public String getState() {
        return state;
    }

    @JsonProperty("state")
    public void setState(String state) {
        this.state = state;
    }

    public Address withState(String state) {
        this.state = state;
        return this;
    }

    @JsonProperty("country")
    public String getCountry() {
        return country;
    }

    @JsonProperty("country")
    public void setCountry(String country) {
        this.country = country;
    }

    public Address withCountry(String country) {
        this.country = country;
        return this;
    }

    @JsonProperty("zipCode")
    public Integer getZipCode() {
        return zipCode;
    }

    @JsonProperty("zipCode")
    public void setZipCode(Integer zipCode) {
        this.zipCode = zipCode;
    }

    public Address withZipCode(Integer zipCode) {
        this.zipCode = zipCode;
        return this;
    }

    @JsonProperty("contactNumber")
    public Object getContactNumber() {
        return contactNumber;
    }

    @JsonProperty("contactNumber")
    public void setContactNumber(Object contactNumber) {
        this.contactNumber = contactNumber;
    }

    public Address withContactNumber(Object contactNumber) {
        this.contactNumber = contactNumber;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("streetAddress", streetAddress).append("city", city).append("state", state).append("country", country).append("zipCode", zipCode).append("contactNumber", contactNumber).toString();
    }

}

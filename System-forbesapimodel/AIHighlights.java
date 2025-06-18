package com.forbes.forbesapi.model;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import dev.morphia.annotations.Embedded;
import lombok.Data;




@Data
@Embedded
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AIHighlights implements Serializable{
    private String natrualId;
    private Date publishedDate;
    private List<Highlights> highlights;

}

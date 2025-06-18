package com.forbes.forbesapi.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import dev.morphia.annotations.Embedded;
import lombok.Data;

import java.util.List;

@Data
@Embedded
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Highlights implements Serializable {
    private Integer start;
    private Integer end;
    private String link;
    private String text;
    private List<String> bullets;
}

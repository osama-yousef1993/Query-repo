package com.forbes.forbesapi.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import dev.morphia.annotations.Embedded;
import lombok.Data;


@Data
@Embedded
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AI implements Serializable {
    private AISummary summary;
    private AIHighlights highlights;
}

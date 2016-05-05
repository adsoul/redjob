package com.s24.resque.queue;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.util.AnnotatedTypeScanner;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;

/**
 * Scans the classpath and registers JSON subtypes (@link {@link JsonTypeName}) at a {@link ObjectMapper}.
 */
public class PayloadTypeScanner extends AnnotatedTypeScanner {
    /**
     * JSON mapper to register subtypes (@link {@link JsonTypeName}) at.
     */
    private ObjectMapper json;

    /**
     * Base packages to scan.
     */
    private String[] basePackages;

    @PostConstruct
    public void afterPropertiesSet() {
        Assert.notNull(json, "Precondition violated: json != null.");
        Assert.notNull(basePackages, "Precondition violated: basePackages != null.");

        findTypes(basePackages).forEach(json::registerSubtypes);
    }

    /**
     * Constructor.
     */
    public PayloadTypeScanner() {
        super(JsonTypeName.class);
    }

    /**
     * JSON mapper to register subtypes (@link {@link JsonTypeName}) at.
     */
    public ObjectMapper getJson() {
        return json;
    }

    /**
     * JSON mapper to register subtypes (@link {@link JsonTypeName}) at.
     */
    public void setJson(ObjectMapper json) {
        this.json = json;
    }

    /**
     * Base packages to scan.
     */
    public String[] getBasePackages() {
        return basePackages;
    }

    /**
     * Base packages to scan.
     */
    public void setBasePackages(String... basePackages) {
        this.basePackages = basePackages;
    }
}

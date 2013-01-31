package com.mozilla.bagheera.consumer.validation;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

public class JsonValidator implements Validator {
    
    private static final Logger LOG = Logger.getLogger(JsonValidator.class);
    
    private final JsonFactory jsonFactory = new JsonFactory();
    
    @Override
    public boolean isValid(byte[] data) {
        boolean isValid = false;
        JsonParser parser = null;
        try {
            parser = jsonFactory.createJsonParser(data);
            while (parser.nextToken() != null) {
                // noop
            }
            isValid = true;
        } catch (JsonParseException ex) {
            LOG.error("JSON parse error");
        } catch (IOException e) {
            LOG.error("JSON IO error");
        } finally {
            if (parser != null) {
                try {
                    parser.close();
                } catch (IOException e) {
                    LOG.error("Error closing JSON parser", e);
                }
            }
        }
        
        return isValid;
    }

}

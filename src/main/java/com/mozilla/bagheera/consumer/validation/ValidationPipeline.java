package com.mozilla.bagheera.consumer.validation;

import java.util.LinkedList;
import java.util.List;

public class ValidationPipeline {

    private List<Validator> validators = new LinkedList<Validator>();
    
    public void addFirst(Validator validator) {
        validators.add(0, validator);
    }
    
    public void addLast(Validator validator) {
        validators.add(validator);
    }
    
    public boolean isValid(byte[] data) {
        boolean valid = false;
        for (Validator validator : validators) {
            valid = validator.isValid(data);
            if (!valid) {
                break;
            }
        }
        
        return valid;
    }
    
}

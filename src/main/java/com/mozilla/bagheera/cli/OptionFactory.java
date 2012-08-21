package com.mozilla.bagheera.cli;

public class OptionFactory {

    private static OptionFactory INSTANCE;
    
    private OptionFactory() {    
    }
    
    public static OptionFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new OptionFactory();
        }
        
        return INSTANCE;
    }
    
    public Option create(String opt, String longOpt, boolean hasArg, String description) {
        return new Option(opt, longOpt, hasArg, description);
    }

}

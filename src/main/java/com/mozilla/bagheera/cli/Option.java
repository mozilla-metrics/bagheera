package com.mozilla.bagheera.cli;

public class Option extends org.apache.commons.cli.Option {

    private static final long serialVersionUID = 4764162254116593978L;

    public Option(String opt, String longOpt, boolean hasArg, String description) {
        this(opt, longOpt, hasArg, description, false);
    }
    
    public Option(String opt, String longOpt, boolean hasArg, String description, boolean required) {
        super(opt, longOpt, hasArg, description);
        setRequired(required);
    }
    
    public Option required() {
        setRequired(true);
        return this;
    }
}

package com.amarsoft.rwa.engine.exception;

public class ParamConfigException extends RuntimeException {

    private static final long serialVersionUID = 8234237673124315L;

    public ParamConfigException(){
        super();
    }

    public ParamConfigException(String message) {
        super(message);
    }

    public ParamConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParamConfigException(Throwable cause) {
        super(cause);
    }

}

package ar.edu.itba.pod.client.exceptions;

import java.util.function.Supplier;

public class RequiredPropertyException extends Exception {
    public RequiredPropertyException(String message) {
        super(message);
    }

}

package ar.edu.itba.pod.models;

import java.io.Serializable;

public class Movement implements Serializable {
    private static final long serialVersionUID = 1L;
    private static long quantity = 0;
    private long id;
    private String movementType;
    private String flightClass;
    private String origin;
    private String destination;
    private String airline;
    private String clasification;


    public Movement(String type, String flightClass, String origin, String destination,
                    String airline, String clasification) {
        movementType = type;
        this.flightClass = flightClass;
        this.origin = origin;
        this.destination = destination;
        this.airline = airline;
        this.clasification = clasification;
        id = quantity++;
    }

    public String getClasification() {
        return clasification;
    }

    public String getMovementType() {
        return movementType;
    }

    public String getFlightClass() {
        return flightClass;
    }

    public String getOrigin() {
        return origin;
    }

    public String getDestination() {
        return destination;
    }

    public String getAirline() {
        return airline;
    }

    public long getId() {
        return id;
    }

    //TODO make hashcode
    //TODO after asking if there can be repeated flights on movimientos.csv
}

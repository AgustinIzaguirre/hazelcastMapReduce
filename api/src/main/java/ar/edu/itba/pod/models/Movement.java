package ar.edu.itba.pod.models;

import java.io.Serializable;

public class Movement implements Serializable {
    //TODO add serializable UID
    private String movementType;
    private String flightClass;
    private String origin;
    private String destination;
    private String airline;
    private String clasification;
    private String date;
    private String time;

    public Movement(String type, String flightClass, String origin, String destination,
                    String airline, String clasification, String date, String time) {
        movementType = type;
        this.flightClass = flightClass;
        this.origin = origin;
        this.destination = destination;
        this.airline = airline;
        this.clasification = clasification;
        this.date = date;
        this.time = time;
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
    public String getDate() {
        return date;
    }
    public String getTime() {
        return time;
    }
    public boolean equals(Object other) {
        if(other == null || !other.getClass().equals(getClass())) {
            return false;
        }

        Movement otherMovement = (Movement)other;
        return movementType.equals(otherMovement.getMovementType()) &&
                flightClass.equals(otherMovement.getFlightClass()) &&
                origin.equals(otherMovement.getOrigin()) &&
                destination.equals(otherMovement.getDestination()) &&
                airline.equals(otherMovement.getAirline()) &&
                date.equals(otherMovement.getDate()) &&
                time.equals(otherMovement.getTime());
    }

    //TODO make hashcode
    //TODO after asking if there can be repeated flights on movimientos.csv
}

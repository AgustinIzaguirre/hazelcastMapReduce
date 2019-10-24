package ar.edu.itba.pod.results;

import java.io.Serializable;

public class AirportPairResult implements Serializable{
    private static final long serialVersionUID = 1L;
    private long movements;
    private String first;
    private String second;

    public AirportPairResult(long movements, String first, String second) {
        this.movements = movements;
        this.first = first.compareTo(second) < 0 ? first : second;
        this.second = first.compareTo(second) < 0 ? second : first;
    }

    public long getMovements() {
        return movements;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object other) {
        if(other == null || !other.getClass().equals(getClass())) {
            return false;
        }

        AirportPairResult otherAirportPair = (AirportPairResult)other;

        return first.equals(otherAirportPair.getFirst()) &&
                second.equals(otherAirportPair.getSecond()) &&
                movements == otherAirportPair.getMovements(); //if other 2 are equal this should be equal also
    }
}

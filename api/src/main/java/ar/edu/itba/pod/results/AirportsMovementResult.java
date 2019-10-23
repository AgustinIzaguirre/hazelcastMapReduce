package ar.edu.itba.pod.results;

public class AirportsMovementResult {
    private String key;
    private Long movements;

    public AirportsMovementResult(String key, Long movements) {
        this.key = key;
        this.movements = movements;
    }

    public String getKey() {
        return key;
    }

    public Long getMovements() {
        return movements;
    }
}

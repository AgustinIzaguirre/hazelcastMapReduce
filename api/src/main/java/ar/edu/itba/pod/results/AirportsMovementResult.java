package ar.edu.itba.pod.results;

import java.io.Serializable;

public class AirportsMovementResult implements Serializable {
    private static final long serialVersionUID = 1L;
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

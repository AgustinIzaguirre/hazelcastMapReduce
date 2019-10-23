package ar.edu.itba.pod.results;

public class AirportsMovementResult {
    private String oaciCode;
    private Long movements;

    public AirportsMovementResult(String oaciCode, Long movements) {
        this.oaciCode = oaciCode;
        this.movements = movements;
    }

    public String getOaciCode() {
        return oaciCode;
    }

    public Long getMovements() {
        return movements;
    }
}

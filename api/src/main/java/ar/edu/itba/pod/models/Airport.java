package ar.edu.itba.pod.models;

import java.io.Serializable;

public class Airport implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private String province;
    private String oaciCode;

    public Airport(String name, String province, String oaciCode) {
        this.name = name;
        this.province = province;
        this.oaciCode = oaciCode;
    }

    public String getName() {
        return name;
    }

    public String getProvince() {
        return province;
    } //TODO maybe delete on final version

    public String getOaciCode() {
        return oaciCode;
    }
}

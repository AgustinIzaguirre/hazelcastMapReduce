package ar.edu.itba.pod.models;

import java.io.Serializable;

public class Airport implements Serializable {
//TODO add serial UID
    private String name;
    private String localCode; //TODO check if necessary
    private String province;
    private String oaciCode;

    public Airport(String name, String localCode, String province, String oaciCode) {
        this.name = name;
        this.localCode = localCode;
        this.province = province;
        this.oaciCode = oaciCode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocalCode() {
        return localCode;
    }

    public void setLocalCode(String localCode) {
        this.localCode = localCode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getOaciCode() {
        return oaciCode;
    }

    public void setOaciCode(String oaciCode) {
        this.oaciCode = oaciCode;
    }
}

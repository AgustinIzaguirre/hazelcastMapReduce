package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Airport;
import com.hazelcast.core.IMap;

import java.io.*;

public class FileLoader {
    //airports indexes
    private int airportNameIndex;
    private int airportLocalCodeIndex;
    private int airportProvinceIndex;
    private int airportOaciCodeIndex;

    //movements indexes

    public void loadAirports(String path, IMap<String, Airport> map) throws IOException {
        FileReader airportsFile = new FileReader(path);
        BufferedReader airportReader = new BufferedReader(airportsFile);
        String line = airportReader.readLine();
        String airportData[] = line.split(";");
        loadAirportIndexes(airportData);
        do {
            line = airportReader.readLine();
            if(line != null) {
                airportData = line.split(";");
                Airport currentAirport = loadAirport(airportData);
                if(currentAirport != null) { //TODO ADD optional
                    map.put(currentAirport.getOaciCode(), currentAirport);
                }
            }
        } while(line != null);

        System.out.println("airportLocalCodeIndex: " + airportLocalCodeIndex);
        System.out.println("airportOaciCodeIndex: " + airportOaciCodeIndex);
        System.out.println("airportNameIndex: " + airportNameIndex);
        System.out.println("airportProvinceIndex: " + airportProvinceIndex);
    }

    private void loadAirportIndexes(String[] airportData) {
        for(int i = 0; i < airportData.length; i++) {
            String currentType = airportData[i].toUpperCase(); //TODO maybe compare with the csv actual name
            if(currentType.equals("LOCAL")) {
                airportLocalCodeIndex = i;
            }
            else if(currentType.equals("OACI")) {
                airportOaciCodeIndex = i;
            }
            else if(currentType.equals("DENOMINACION")) {
                airportNameIndex = i;
            }
            else if(currentType.equals("PROVINCIA")) {
                airportProvinceIndex = i;
            }
        }
    }

    private Airport loadAirport(String[] airportData) {
        String name = null, localCode = null, province = null, oaciCode = null;
        if(airportData[airportOaciCodeIndex].length() > 0) {
            oaciCode = airportData[airportOaciCodeIndex];
        }
        else {
            return null;
        }
        if(airportData[airportNameIndex].length() > 0) {
            name = airportData[airportNameIndex];
        }
        if(airportData[airportLocalCodeIndex].length() > 0) {
            localCode = airportData[airportLocalCodeIndex];
        }
        if(airportData[airportProvinceIndex].length() > 0) {
            province = airportData[airportProvinceIndex];
        }

        return new Airport(name, localCode, province, oaciCode);
    }
}


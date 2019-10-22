package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import com.hazelcast.core.IMap;

import java.io.*;

public class FileLoader {
    //airports indexes
    private int airportNameIndex;
    private int airportLocalCodeIndex;
    private int airportProvinceIndex;
    private int airportOaciCodeIndex;

    //movements indexes
    private int movementTypeIndex;
    private int flightClassIndex;
    private int originIndex;
    private int destinationIndex;
    private int airlineIndex;
    private int clasificationIndex;

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

    public void loadMovements(String path, IMap<Long, Movement> map) throws IOException {
        FileReader movementsFile = new FileReader(path);
        BufferedReader movementReader = new BufferedReader(movementsFile);
        String line = movementReader.readLine();
        String movementData[] = line.split(";");
        loadMovementIndexes(movementData);
        do {
            line = movementReader.readLine();
            if(line != null) {
                movementData = line.split(";");
                Movement currentMovement = loadMovement(movementData);
                if(currentMovement != null) { //TODO ADD optional
                    map.put(currentMovement.getId(), currentMovement);
                }
            }
        } while(line != null);
    }



    private void loadMovementIndexes(String[] movementData) {
        for(int i = 0; i < movementData.length; i++) {
            String currentType = movementData[i];
            if(currentType.equals("Origen OACI")) {
                originIndex = i;
            }
            else if(currentType.equals("Destino OACI")) {
                destinationIndex = i;
            }
            else if(currentType.equals("Aerolinea Nombre")) {
                airlineIndex = i;
            }
            else if(currentType.equals("Clase de Vuelo")) {
                flightClassIndex = i;
            }
            else if(currentType.equals("Clasificación Vuelo")) {
                clasificationIndex = i;
            }
            else if(currentType.equals("Tipo de Movimiento")) {
                movementTypeIndex = i;
            }
        }
    }
    private Movement loadMovement(String[] movementData) {
        String origin = null, destination = null, airline = null;
        String flightClass = null, clasification = null, type = null;
        if(movementData[originIndex].length() > 0) {
            origin = movementData[originIndex];
        }
        if(movementData[destinationIndex].length() > 0) {
            destination = movementData[destinationIndex];
        }
        if(movementData[airlineIndex].length() > 0) {
            airline = movementData[airlineIndex];
        }
        if(movementData[flightClassIndex].length() > 0) {
            flightClass = movementData[flightClassIndex];
        }
        if(movementData[clasificationIndex].length() > 0) {
            clasification = movementData[clasificationIndex];
        }
        if(movementData[movementTypeIndex].length() > 0) {
            type = movementData[movementTypeIndex];
        }
        if(origin == null || destination == null) {
            return null;
        }

        return new Movement(type, flightClass, origin, destination, airline, clasification);
    }

}


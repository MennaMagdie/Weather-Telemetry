package com.weather;

public class WeatherStatusMessage {
    private long station_id;
    private long s_no;
    private String battery_status;
    private long status_timestamp;
    private WeatherDetails weather;

    public WeatherStatusMessage() {}

    public WeatherStatusMessage(long station_id, long s_no, String battery_status, long status_timestamp, WeatherDetails weather) {
        // THESE ARE THE ENVELOPE METADATA
        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;

        //THIS IS THE PAYLOAD
        this.weather = weather;
    }

    public long getStation_id() { return station_id; }
    public long getS_no() { return s_no; }
    public String getBattery_status() { return battery_status; }
    public long getStatus_timestamp() { return status_timestamp; }
    public WeatherDetails getWeather() { return weather; }
}
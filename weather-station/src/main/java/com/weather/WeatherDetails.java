package com.weather;

public class WeatherDetails {
    private int humidity;
    private int temperature;
    private int wind_speed;

    public WeatherDetails() {}

    public WeatherDetails(int humidity, int temperature, int wind_speed) {
        this.humidity = humidity;
        this.temperature = temperature;
        this.wind_speed = wind_speed;
    }

    public int getHumidity() { return humidity; }
    public int getTemperature() { return temperature; }
    public int getWind_speed() { return wind_speed; }
}
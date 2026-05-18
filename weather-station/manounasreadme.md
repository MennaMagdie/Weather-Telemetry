from weather-station

mvn clean compile

mvn exec:java -Dexec.mainClass="com.weather.WeatherStationMock"


       ┌─────────────────────────────────────────────────────────┐
       │                      CENTRAL STATION APP                │
       │                                                         │
 ──────┼─────────┐             ┌──────────────┐                  │
 Kafka │ Consumer│ ──────────> │   Bitcask    │ (Your teammate's │
 Stream│ Loop    │  .put()     │Storage Engine│  code goes here) │
 ──────┼─────────┘             └──────────────┘                  │
       │                              │                          │
       │                              ▼                          │
       │                       ┌──────────────┐                  │
       │                       │  Web Server  │ <─── Client bash │
       │                       │  (Endpoints) │      script tool │
       └───────────────────────┴──────────────┴──────────────────┘


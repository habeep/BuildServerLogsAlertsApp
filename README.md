# BuildServerLogsAlertsApp

1. Before run this, Palease copy the file JSON/events.json to your local computer

2. How to check whether gradale is existing or not

gradle -version

3. how to run this 
  a. go to BuildServerLogsAlertsApp location
  b. execute below command
      gradlew clean run --args='{JSON fileLocation}'
      For Example
      gradlew clean run --args='d:\\events.json'



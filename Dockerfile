FROM amazoncorretto:17.0.6

COPY target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java","-jar","/app.jar","8080" , "./data"]

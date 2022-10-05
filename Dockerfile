FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

COPY target/transitdata-cache-bootstrapper.jar /usr/app/transitdata-cache-bootstrapper.jar

ENTRYPOINT ["java", "-XX:InitialRAMPercentage=10.0", "-XX:MaxRAMPercentage=95.0", "-jar", "/usr/app/transitdata-cache-bootstrapper.jar"]

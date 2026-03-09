FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

ARG JAR_FILE

COPY ${JAR_FILE} app.jar

ENTRYPOINT ["java","-jar","/app/app.jar"]
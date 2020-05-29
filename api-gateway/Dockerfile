FROM adoptopenjdk/openjdk11:alpine-jre
RUN addgroup -S stellio && adduser -S stellio -G stellio
USER stellio:stellio
ARG JAR_FILE=build/libs/*.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]


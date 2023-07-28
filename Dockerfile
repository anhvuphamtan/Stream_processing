FROM maven:3.8.6-openjdk-11 AS build 

COPY . /app 

WORKDIR /app/ 

RUN mvn clean
RUN mvn install dependency:copy-dependencies

FROM openjdk:11.0.11-jre-slim

COPY --from=build /app/ /app 

WORKDIR /app/

RUN apt-get update
RUN apt-get -y install cron

RUN crontab -l | { cat; echo "* * * * * /app/src/main/java/com/postgres/delete_old_data.sh"; } | crontab -

CMD ["java", "-classpath", "target/maven.jar:target/dependency/*", "com.Main"]
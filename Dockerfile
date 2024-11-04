FROM openjdk:11

COPY . /incubator-wayang

WORKDIR /incubator-wayang

RUN ./mvnw clean install \
  -DskipTests \
  -Drat.skip=true # skip checks for licensed files because Dockerfile isn't approved

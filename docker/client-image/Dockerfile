###############################################################################
# Build the application
###############################################################################

FROM maven:3.6-jdk-8-slim AS builder

# Get the application and compile it
COPY ./java/flink-new-york-school-buses /opt/flink-new-york-school-buses
WORKDIR /opt/flink-new-york-school-buses
#RUN mvn clean install

###############################################################################
# Build Operations Playground Image
###############################################################################

FROM flink:1.10.0

WORKDIR /opt/flink/bin

# Copy Job
COPY --from=builder /opt/flink-new-york-school-buses/target/flink-new-york-school-buses-*.jar /opt/NYSchoolBusJob.jar

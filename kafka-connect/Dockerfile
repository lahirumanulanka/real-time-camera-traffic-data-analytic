FROM confluentinc/cp-kafka-connect:7.6.1

# Install the JDBC sink/source connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest

# (Optional) Additional plugins can be added similarly:
# RUN confluent-hub install --no-prompt debezium/debezium-connector-postgres:latest


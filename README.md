# kafka-demo

# Prerequistes

 It is assumed that java 21 installed in the machine <br/>
 Kafka kafka_2.13-4.1.0 is available to scripts in the bin <br/>
 Linux enviornment in the machine to run shell scripts (Example : git bash) <br/>

# Building the project

 mvn clean package <br/>

# Running the project

  1. Start Docker containers docker-compose up -d <br/>
  2. Create the topics <br/>
  bin/kafka-topics.sh --create --topic iotinputtopic --bootstrap-server localhost:29092 <br/>
  bin/kafka-topics.sh --create --topic iotoutputtopic --bootstrap-server localhost:29092 <br/>
  3. Run the transaction service <br/>
  mvn exec:java -Dexec.mainClass=iot.TransactionalClient <br/>

# Pushing the messages to the topic
 bin/kafka-console-producer.sh --topic iotinputtopic --bootstrap-server localhost:29092 --property "parse.key=true" --property "key.separator=:" <br/>

 Example message:message <br/>







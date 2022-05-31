# Exam-kafka-streams

The solution for challenge 2 form the Scala academy exam.

The application reads data from two topics. In the first topic are words. In the second one are integer numbers. Words are transformed to upper case letters. The positive numbers are multiplied by a number given on command line and the negative numbers get prefix “negative number: “. All results are written to separate output topics.

## What you need

* Java 11
* Scala 2.13.8
* Sbt 1.6.2
* Kafka 3.2

## How to run

* Before running the application you must create input topics for words and numbers and output topics for words, positive numbers and negative numbers. You can create topics by typing the command
`kafka-topics.sh --create --topic <topic name> --bootstrap-server localhost:9092`  
  in the linux command shell which is opened in a folder with the Kafka
* In order to run the application enter the command `sbt 'run <list of parametrs>'`
* In order to get list of necessary parameters and their descriptions please enter `sbt 'run --help'`
* In order to produce input records and read output records you can use, for example Kafka console consumers and producers.

## How to run test
* In order to run tests enter the command `sbt test `

## FAQ
* Remember to run Zookeeper and Kafka server before creating topics.
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala._
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import java.util.Properties
import scopt.OParser

object KafkaStreamsApp {
  def main(args: Array[String]): Unit = {

    OParser.parse(getParser, args, Config()) match {
      case Some(config) => {
        val props = new Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "words-numbers-pipe")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

        val stream = new KafkaStreams(getTopology(config), props)
        stream.start()
      }
      case None => println("Invalid configuration")
    }
  }

  def getTopology(config: Config): Topology = {
    val builder = new StreamsBuilder()

    val words = builder.stream[String, String](config.wordsInput)
    words.mapValues(word => word.toUpperCase).to(config.wordsOutput)

    val numbers = builder.stream[String, String](config.numbersInput)
    val positiveNumbers = numbers.filter { case (_, value) => value.toInt >= 0 }
    val negativeNumbers = numbers.filter { case (_, value) => value.toInt < 0 }

    positiveNumbers.mapValues(number => (number.toInt * config.multiplier).toString).to(config.positiveNumbersOutput)
    negativeNumbers.mapValues(number => "negative number: " + number).to(config.negativeNumbersOutput)

    builder.build()
  }

  private def getParser: OParser[String, Config] = {
    val argsBuilder = OParser.builder[Config]
    val parser = {
      import argsBuilder._
      OParser.sequence(
        arg[String]("<Input topic for words>")
          .required()
          .action {(name, config) => config.copy(wordsInput = name)}
          .text("Name of the input topic for words"),
        arg[String]("<Input topic for numbers>")
          .required()
          .action {(name, config) => config.copy(numbersInput = name)}
          .text("Name of the input topic for numbers"),
        arg[String]("<Output topic for words>")
          .required()
          .action {(name, config) => config.copy(wordsOutput = name)}
          .text("Name of the output topic for words"),
        arg[String]("<Output topic for  positive numbers>")
          .required()
          .action {(name, config) => config.copy(positiveNumbersOutput = name)}
          .text("Name of the output topic for positive numbers"),
        arg[String]("<Output topic for negative numbers>")
          .required()
          .action {(name, config) => config.copy(negativeNumbersOutput = name)}
          .text("Name of the output topic for negative numbers"),
        arg[Double]("<Multiplier for positive numbers>")
          .required()
          .action {(name, config) => config.copy(multiplier = name)}
          .text("Value of the multiplier for positive numbers"),
        help("help").text("prints this usage text")
      )
    }
    parser
  }
}




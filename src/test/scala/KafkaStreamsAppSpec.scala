import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.apache.kafka.common.serialization.Serdes

import java.util.Properties

class KafkaStreamsAppSpec extends AnyFlatSpec with should.Matchers {
  def getFixture = new {
    val props = new Properties()
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

    val stringSerde =  new Serdes.StringSerde()

    val config = Config("words-input", "numbers-input", "words-output", "positive-output", "negative-output", 2)

    val testDriver = new TopologyTestDriver(KafkaStreamsApp.getTopology(config), props)

    val wordsInputTopic: TestInputTopic[String, String] = testDriver
      .createInputTopic(config.wordsInput, stringSerde.serializer(), stringSerde.serializer())
    val numbersInputTopic: TestInputTopic[String, String] = testDriver
      .createInputTopic(config.numbersInput, stringSerde.serializer(), stringSerde.serializer())

    val wordsOutputTopic: TestOutputTopic[String, String] =  testDriver
      .createOutputTopic(config.wordsOutput, stringSerde.deserializer(), stringSerde.deserializer())
    val positiveNumbersOutputTopic: TestOutputTopic[String, String] =  testDriver
      .createOutputTopic(config.positiveNumbersOutput, stringSerde.deserializer(), stringSerde.deserializer())
    val negativeNumbersOutputTopic: TestOutputTopic[String, String] =  testDriver
      .createOutputTopic(config.negativeNumbersOutput, stringSerde.deserializer(), stringSerde.deserializer())
  }

  behavior of "Topology"

  it should "return ALA for input ala in the topic words-input" in {
//    Arrange
    val word = "ala"
    val f = getFixture
//    Act
    f.wordsInputTopic.pipeInput("ala")
//    Assert
    f.wordsOutputTopic.readValue() shouldBe word.toUpperCase
  }

  it should
    """return 24.0 in positive-output topic and negative number: -2 in negative-output topic
      | for input 12 -2 in the topic number-input""".stripMargin in {
    //    Arrange
    val word = "ala"
    val f = getFixture
    //    Act
    f.numbersInputTopic.pipeInput("12")
    f.numbersInputTopic.pipeInput("-2")

    //    Assert
    f.positiveNumbersOutputTopic.readValue() shouldBe "24.0"
    f.negativeNumbersOutputTopic.readValue() shouldBe "negative number: -2"
  }
}

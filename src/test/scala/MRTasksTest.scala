
import org.scalatest.*
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.*
import com.typesafe.config.{Config, ConfigFactory}
import tasks.HelperUtils.{OtherUtils, TimeUtil}

import java.util.regex.Pattern

class MRTasksTest extends AnyFunSpec {
  // loading config references and regex patterns from it
  val configReference: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")
  val logPattern: Pattern = Pattern.compile(configReference.getString("LogPattern"))
  val stringMessagePattern: Pattern = Pattern.compile(configReference.getString("StringMessagePattern"))
  val intermediateInputDataPattern: Pattern = Pattern.compile(configReference.getString("IntermediateTaskOutputPattern"))

  describe("Testing Log Message Pattern") {

    it("should test if a single log entry matches the log Pattern") {
      val logEntry = "13:05:54.942 [scala-execution-context-global-17] DEBUG HelperUtils.Parameters$ - AdEbEe[?J:)K]x!]=N8&qq(,]Q/:@2#O{@t<Ei$9?\\#FO)qG@h"
      val matcher = logPattern.matcher(logEntry)
      matcher.matches() shouldBe true
    }

    it("should extract the log time from log entry") {
      val logEntry = "13:05:54.942 [scala-execution-context-global-17] DEBUG HelperUtils.Parameters$ - AdEbEe[?J:)K]x!]=N8&qq(,]Q/:@2#O{@t<Ei$9?\\#FO)qG@h"
      val matcher = logPattern.matcher(logEntry)
      matcher.matches()

      matcher.group(1) shouldBe "13:05:54.942"
    }

    it("should extract the log message type from log entry") {
      val logEntry = "13:05:54.942 [scala-execution-context-global-17] DEBUG HelperUtils.Parameters$ - AdEbEe[?J:)K]x!]=N8&qq(,]Q/:@2#O{@t<Ei$9?\\#FO)qG@h"
      val matcher = logPattern.matcher(logEntry)
      matcher.matches()

      matcher.group(3) shouldBe "DEBUG"
    }

    it("should extract the string message from log entry") {
      val logEntry = "13:05:54.942 [scala-execution-context-global-17] DEBUG HelperUtils.Parameters$ - AdEbEe[?J:)K]x!]=N8&qq(,]Q/:@2#O{@t<Ei$9?\\#FO)qG@h"
      val matcher = logPattern.matcher(logEntry)
      matcher.matches()

      matcher.group(5) shouldBe "AdEbEe[?J:)K]x!]=N8&qq(,]Q/:@2#O{@t<Ei$9?\\#FO)qG@h"
    }
  }

  describe("String instance Pattern matching") {

    it("should match the string instance pattern if present in log message") {
      val logMessage = "5B8`R=ft<xfO0]R+r$*I:H/^>q,kag1ag3bf3M7nag3cf0,g@\"93>q0Md&O^s0:)kS*siv^zCP"
      val matcher = stringMessagePattern.matcher(logMessage)

      matcher.matches() shouldBe true
    }

    it("should not match the string instance pattern if not present in log message") {
      val logMessage = "KoBfR#8[U^IC*$RB]/\\4)8H)F}J%BtECPRmq)`_tRE7h~EdKomdiap!os0M?"
      val matcher = stringMessagePattern.matcher(logMessage)

      matcher.matches() shouldBe false
    }
  }

  describe("Test string encoding and decoding utilities") {

    it("should encode two strings into a single string") {
      val input1 = "10"
      val input2 = "20"
      val delimiter = OtherUtils.encodeDecodeDelimiter

      val expectedOutput = input1.concat(delimiter).concat(input2)
      val result = OtherUtils.encode(input1, input2)

      result shouldBe expectedOutput
    }

    it("should decode a valid single string") {
      val input = "10|20"
      OtherUtils.decode(input) shouldBe ("10", "20")
    }

    it("should decode an invalid single string into default case") {
      val input = "xyz"
      OtherUtils.decode(input) shouldBe ("0", "0")
    }
  }

  describe("Test time utils") {

    it("should test the time interval between two times") {
      val input1 = "13:05:54.942"
      val input2 = "13:05:57.468"
      val expectedOutput: Long = 2526L

      TimeUtil.getInterval(input1, input2) shouldBe expectedOutput
    }

    it("should convert the input time of log message to HH:SS-HH:SS format based on given interval - 60 seconds for this test case") {
      val input = "13:05:54.942"
      val intervalInSeconds = 60

      TimeUtil.convertToHourMinuteInterval(input, intervalInSeconds) shouldBe ("13:05:00","13:06:00")
    }

    it("should convert the input time of log message to HH:SS-HH:SS format based on given interval 20 seconds for this test case") {
      val input = "13:05:54.942"
      val intervalInSeconds = 20

      TimeUtil.convertToHourMinuteInterval(input, intervalInSeconds) shouldBe ("13:05:40","13:06:00")
    }
  }

  describe("Testing Second task key and value extractor pattern") {

    it("Should match the pattern with the key of final reducer for task 2") {
      val input = "13:05:40-13:06:00,30"
      val matcher = intermediateInputDataPattern.matcher(input)

      matcher.matches() shouldBe true
    }

    it("Should match extract the interval from the key of final reducer for task 2 using the pattern detector") {
      val input = "13:05:40-13:06:00,30"
      val matcher = intermediateInputDataPattern.matcher(input)

      matcher.matches()
      matcher.group(1) shouldBe "13:05:40-13:06:00"
    }

    it("Should match extract the error count from the key of final reducer for task 2 using the pattern detector") {
      val input = "13:05:40-13:06:00,30"
      val matcher = intermediateInputDataPattern.matcher(input)

      matcher.matches()
      matcher.group(2) shouldBe "30"
    }
  }

}

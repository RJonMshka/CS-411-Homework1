package tasks.HelperUtils

import java.text.SimpleDateFormat
import java.util.Date

object TimeUtil {
  val logTimeFormatter = SimpleDateFormat("HH:mm:ss.SSS")
  val hourMinutesTimeFormatter = SimpleDateFormat("HH:mm")
  val millisecondsInSeconds = 1000
  def getInterval(s1: String, s2: String): Long = logTimeFormatter.parse(s2).getTime() - logTimeFormatter.parse(s1).getTime()

  def convertToHourMinuteInterval(s1: String, intervalInSeconds: Int): (String, String) =
    val divider = intervalInSeconds * millisecondsInSeconds
    val time = (logTimeFormatter.parse(s1).getTime() / divider) * divider
    (hourMinutesTimeFormatter.format(new Date(time)).toString, hourMinutesTimeFormatter.format(new Date(time + divider)).toString)
}

package tasks.HelperUtils

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TimeUtil {
  val timeFormatter = SimpleDateFormat("HH:mm:ss.SSS")
  def getInterval(s1: String, s2: String): Long = timeFormatter.parse(s2).getTime() - timeFormatter.parse(s1).getTime()
}

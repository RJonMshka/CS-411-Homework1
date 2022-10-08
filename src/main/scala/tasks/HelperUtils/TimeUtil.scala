package tasks.HelperUtils

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Wrapper for time utils
 */
object TimeUtil {
  // format to match the log message time
  val logTimeFormatter: SimpleDateFormat = SimpleDateFormat("HH:mm:ss.SSS")
  // format to store the time interval as key for second task specifically
  val hourMinutesTimeFormatter: SimpleDateFormat = SimpleDateFormat("HH:mm:ss")
  // number of milliseconds in one second
  val millisecondsInSeconds: Int = 1000

  /**
   * This method calculate the time difference in milliseconds between two time string in "HH:mm:ss.SSS" format
   * @param s1 - first time string
   * @param s2 - second time string
   * @return - time difference in milliseconds
   */
  def getInterval(s1: String, s2: String): Long = logTimeFormatter.parse(s2).getTime - logTimeFormatter.parse(s1).getTime


  /**
   * This method convert a time string in "HH:mm:ss.SSS" into a tuple of two "HH:mm:ss" formatted time strings
   * @param s1 - The time string that needs to be converted
   * @param intervalInSeconds - the interval (bin) in which the time needs to be converted - in seconds
   * @return Tuple2 of String, String that represents two times separated by "intervalInSeconds" param passed
   */
  def convertToHourMinuteInterval(s1: String, intervalInSeconds: Int): (String, String) =
    val divider = intervalInSeconds * millisecondsInSeconds
    val time = (logTimeFormatter.parse(s1).getTime / divider) * divider
    (hourMinutesTimeFormatter.format(new Date(time)), hourMinutesTimeFormatter.format(new Date(time + divider)))
}

package tasks.HelperUtils

object OtherUtils {
  val encodeDecodeDelimiter = "|"

  def encode(first: String, second: String ): String =
    first.concat(encodeDecodeDelimiter).concat(second)

  def decode(encodedStr: String): (String, String) =
    encodedStr.split("\\" + encodeDecodeDelimiter) match
      case Array(a, b) => (a, b)
      case _ => ("0", "0")
}

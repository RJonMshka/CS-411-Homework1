package tasks.HelperUtils


/**
 * Wrapper for encoding and decoding of two strings
 */
object OtherUtils {
  // delimiter for encoding
  val encodeDecodeDelimiter = "|"

  /**
   * This method encodes two strings with the delimiter "encodeDecodeDelimiter"
   * @param first - first string to encode
   * @param second - second string
   * @return
   */
  def encode(first: String, second: String ): String =
    first.concat(encodeDecodeDelimiter).concat(second)

  /**
   * This method decode the string into two strings
   * @param encodedStr - encoded string which needs to be decoded
   * @return - Tuple2(String, String) - a tuple of two decoded strings
   */
  def decode(encodedStr: String): (String, String) =
    encodedStr.split("\\" + encodeDecodeDelimiter) match
      case Array(a, b) => (a, b)
      case _ => ("0", "0")
}

package tasks.HelperUtils

object OtherUtils {
  val encodeDecodeDelimiter = "|"

  def encode(first: String, second: String ): String =
    first.concat(encodeDecodeDelimiter).concat(second)

  def decode(encodedStr: String): (String, String) =
    encodedStr.split("\\" + encodeDecodeDelimiter) match
      case Array(a, b) => (a, b)


  def main(args: Array[String]): Unit = {
    println(decode("40|30"))
  }

}

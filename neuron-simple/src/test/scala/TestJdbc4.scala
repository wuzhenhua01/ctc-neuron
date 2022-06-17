import java.time.LocalDate
import java.time.format.DateTimeFormatter

object TestJdbc4 {
  def main(args: Array[String]): Unit = {
    println(DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now()))
  }
}

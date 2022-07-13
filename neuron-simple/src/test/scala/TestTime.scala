import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

object TestTime {
  def main(args: Array[String]): Unit = {
    val createDate = DateTimeFormatter.BASIC_ISO_DATE.format(LocalDateTime.now())

    println(createDate)
  }
}

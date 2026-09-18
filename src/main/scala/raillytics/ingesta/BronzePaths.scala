package raillytics.ingesta

import java.time.LocalDate

object BronzePaths {
  def l1(bronzeRoot: String, source: String, date: LocalDate, fileName: String): String =
    s"$bronzeRoot/l1-raw/$source/$date/$fileName"

  def l2(bronzeRoot: String, source: String, date: LocalDate): String =
    s"$bronzeRoot/l2/$source/$date/"
}

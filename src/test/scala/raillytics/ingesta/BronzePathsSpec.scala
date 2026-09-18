package raillytics.ingesta

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class BronzePathsSpec extends AnyFlatSpec with Matchers {
  private val date = LocalDate.of(2026, 9, 17)

  "BronzePaths.l1" should "build the raw path from bronzeRoot, source, date and filename" in {
    BronzePaths.l1("s3a://raillytics-bronze", "crtm", date, "sample.csv") shouldBe
      "s3a://raillytics-bronze/l1-raw/crtm/2026-09-17/sample.csv"
  }

  "BronzePaths.l2" should "build the parquet directory path from bronzeRoot, source and date" in {
    BronzePaths.l2("s3a://raillytics-bronze", "crtm", date) shouldBe
      "s3a://raillytics-bronze/l2/crtm/2026-09-17/"
  }

  it should "work with a file:// root too, not just s3a://" in {
    BronzePaths.l1("file:///tmp/bronze", "crtm", date, "sample.csv") shouldBe
      "file:///tmp/bronze/l1-raw/crtm/2026-09-17/sample.csv"
  }
}

package raillytics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AppSpec extends AnyFlatSpec with Matchers {
  "App" should "exist" in {
    App shouldNot be(null)
  }
}

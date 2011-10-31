package recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scalaz._
import Scalaz._
import IterV._
import java.io.File
import FileUtils._

@RunWith(classOf[JUnitRunner])
class FileUtilSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  describe("load data into redis") {
    it("should enumerate the first line") {
      val str = enumFile(new File("/home/debasish/my-projects/recon/src/test/scala/net/debasishg/IterUtilSpec.scala"), head) map (_.run)
      println(str.unsafePerformIO)
    }

    it("should enumerate the whole file") {
      val str = enumFile(new File("/home/debasish/my-projects/recon/src/test/scala/net/debasishg/IterUtilSpec.scala"), repeatHead) map (_.run)
      str.unsafePerformIO.foreach(_.foreach(println))
    }
  }
}


package messageReplay

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import messageReplay.ReplayMessages
import java.io.RandomAccessFile
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import scala.io.Source

/**
 * Created by trilok on 3/28/14.
 */

@RunWith(classOf[JUnitRunner])
class ReplayMessageSuite extends FunSuite {

  implicit val formats = org.json4s.DefaultFormats // required for extracting object out of JSON

  val filePath=getClass.getResource("/ActiveMQLog.txt").getPath

  test("seekDateTimeTest") {
    assert(true)
  }

  test("getNextMessage"){
    val raf=new RandomAccessFile(filePath,"r")
    val replay = new ReplayMessages(null,null)

    var msg=replay.getNextMsg(raf)
    assert(msg.isDefined)
    var parsed = msg.get.extract[MsgFormat]
    assert(parsed.date.equals("2014-03-28 19:13:04"))

    // seek somewhere into the first line
    raf.seek(9)
    msg=replay.getNextMsg(raf)
    parsed = msg.get.extract[MsgFormat]
    assert(parsed.date.equals("2014-03-28 19:13:05")) //should be the second line

    raf.seek(raf.length-7)
    msg=replay.getNextMsg(raf)
    assert(msg.isEmpty)
  }

}

package messageReplay

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import messageReplay.ReplayMessages
import java.io.RandomAccessFile
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import scala.io.Source
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

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
    try{
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
    }finally{
      raf.close()
    }
  }

  test("sequentialfind"){
    val toFind="2014-03-28 19:13:08"
    val dFormat:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dateToFind =  DateTime.parse(toFind,dFormat)
    val raf=new RandomAccessFile(filePath,"r")
    val replay = new ReplayMessages(null,null)
    try{
      // find date that exists
      val pos = replay.sequentialSearch(0,dateToFind,raf)
      assert(pos.isDefined)
      assert(pos.get.getFilePointer > 0)
      assert(replay.parsedMessage(replay.parseLine(raf.readLine).get).parsedDate.equals(dateToFind))

      //find non existant date
     val pos2 = replay.sequentialSearch(0,dateToFind.plusMinutes(50),raf)
      assert(pos2 == None)

      //find a date before anything on the file
      val pos3 = replay.sequentialSearch(0,dateToFind.minusMinutes(50),raf)
      assert(pos3.get.getFilePointer==0)
    }finally{
      raf.close()
    }
  }

}

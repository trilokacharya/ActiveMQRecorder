package ActiveMQRecorder.messageReplay

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.RandomAccessFile
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import ActiveMQRecorder.MessageReplay.FindMessages

/**
 * Created by trilok on 3/28/14.
 */

@RunWith(classOf[JUnitRunner])
class ReplayMessageSuite extends FunSuite {

  implicit val formats = org.json4s.DefaultFormats // required for extracting object out of JSON

  val filePath=getClass.getResource("/ActiveMQLog.txt").getPath

  test("seekDateTimeTestStream") {
    val raf=new RandomAccessFile(filePath,"r")
    var toFind="2014-03-28 19:13:12"
    val dFormat:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    var dateToFind =  DateTime.parse(toFind,dFormat)

    try{
      val replay = new FindMessages(200L) // threshold is 200 bytes within target
      val msgStream1=replay.seekDateTimeStream(dateToFind,raf)
      assert(!msgStream1.isEmpty)
      val msg= msgStream1.head
      assert(msg.parsedDate.equals(dateToFind))

      toFind="2014-03-28 19:13:16" // this exact timestamp doesn't exist, so we should be given the next one in sequence
      dateToFind =  DateTime.parse(toFind,dFormat)
      val expectedTs =  DateTime.parse("2014-03-28 19:13:17",dFormat)
      val msgStream2 = replay.seekDateTimeStream(dateToFind,raf)
      assert(!msgStream2.isEmpty)
      val msg2 = msgStream2.head
      assert(msg2.parsedDate.equals(expectedTs))
    }
    finally{
      raf.close
    }

  }
  test("sequentialfindStream"){
    val toFind="2014-03-28 19:13:08"
    val dFormat:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dateToFind =  DateTime.parse(toFind,dFormat)
    val raf=new RandomAccessFile(filePath,"r")
    val replay = new FindMessages()
    try{
      // find date that exists
      val msgStream = replay.sequentialSearchStream(0,dateToFind,raf)
      assert(!msgStream.isEmpty)
      assert(msgStream.head.parsedDate.equals(dateToFind))

      //find non existent date
      val msgStream2 = replay.sequentialSearchStream(0,dateToFind.plusMinutes(50),raf)
      assert(msgStream2.isEmpty)

      //find a date before anything on the file. So, we should get the first line
      val msgStream3 = replay.sequentialSearchStream(0,dateToFind.minusMinutes(50),raf)
      assert(!msgStream3.isEmpty)
      assert(msgStream3.head.parsedDate.equals(DateTime.parse("2014-03-28 19:13:04",dFormat)))
    }finally{
      raf.close()
    }
  }

  test("seekDateTimeTestIter") {
    val raf=new RandomAccessFile(filePath,"r")
    var toFind="2014-03-28 19:13:12"
    val dFormat:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    var dateToFind =  DateTime.parse(toFind,dFormat)

    try{
      val replay = new FindMessages(200L) // threshold is 200 bytes within target
      val msgIter1=replay.seekDateTimeIter(dateToFind,raf)
      assert(!msgIter1.isEmpty)
      val msg= msgIter1.next
      assert(msg.parsedDate.equals(dateToFind))

      toFind="2014-03-28 19:13:16" // this exact timestamp doesn't exist, so we should be given the next one in sequence
      dateToFind =  DateTime.parse(toFind,dFormat)
      val expectedTs =  DateTime.parse("2014-03-28 19:13:17",dFormat)
      val msgIter2 = replay.seekDateTimeIter(dateToFind,raf)
      assert(!msgIter2.isEmpty)
      val msg2 = msgIter2.next
      assert(msg2.parsedDate.equals(expectedTs))
    }
    finally{
      raf.close
    }
  }

  test("sequentialfindIter"){
    val toFind="2014-03-28 19:13:08"
    val dFormat:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dateToFind =  DateTime.parse(toFind,dFormat)
    val raf=new RandomAccessFile(filePath,"r")
    val replay = new FindMessages()
    try{
      // find date that exists
      val msgIter1 = replay.sequentialSearchIter(0,dateToFind,raf)
      assert(!msgIter1.isEmpty)
      assert(msgIter1.next.parsedDate.equals(dateToFind))

      //find non existent date
      val msgIter2 = replay.sequentialSearchIter(0,dateToFind.plusMinutes(50),raf)
      assert(msgIter2.isEmpty)

      //find a date before anything on the file. So, we should get the first line
      val msgIter3 = replay.sequentialSearchIter(0,dateToFind.minusMinutes(50),raf)
      assert(!msgIter3.isEmpty)
      val nxtMsg =msgIter3.next
      assert(nxtMsg.parsedDate.equals(DateTime.parse("2014-03-28 19:13:04",dFormat)))
      // There should be other elements in the Iterator
      assert(!msgIter3.isEmpty)
    }finally{
      raf.close()
    }
  }
}

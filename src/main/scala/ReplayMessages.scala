/**
 * Created by trilok on 3/26/14.
 */
package messageReplay

import com.github.nscala_time.time.Imports._
import java.io.BufferedReader
import scala.io.Source
import org.json4s.native._
import org.json4s._
import java.io.RandomAccessFile


class ReplayMessages(val startDate:DateTime, val endDate:DateTime) {
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val magicString = "%^#"


  // Find the position of the message that starts at the specified date, within the startPos and endPos boundaries
  def seekDateTime(startPos:Long,endPos:Long,date:DateTime,bufReader:BufferedReader):Option[BufferedReader]={
    if(endPos<=startPos) None // can't find the startdate

    else{
      val midPos=(startPos+endPos)/2
    }
    None
  }

  /**
   * Given a RandomAccessFile object, gets the next message from the file, starting from any arbitrary position.
   * @param raf
   * @return
   */
  def getNextMsg(raf:RandomAccessFile):Option[JValue]={
    def parseLine(line:String):Option[JValue]=
      Some(parseJson(line.substring(magicString.length)))

    var line = raf.readLine()
    if(line.startsWith(magicString)) parseLine(line)
    else{
      if(raf.getFilePointer < raf.length) { // within file boundary
        line = raf.readLine()
        if(!line.startsWith(magicString)) throw new Exception("Invalid line. Doesn't start with "+magicString)
        parseLine(line)
      }
      else None
    }
  }

  /**
   * Date Comparison between a DateTime object and a String in the expected datetime format
   * @param dtStr
   * @param date
   * @return
   */
  def compareDate(dtStr:String,date:DateTime ):Int={
    val parsedDate=dateFormatter.parseDateTime(dtStr)
    if(parsedDate<date) -1
    else if(parsedDate>date) 1
    else 0 // equal
  }
}

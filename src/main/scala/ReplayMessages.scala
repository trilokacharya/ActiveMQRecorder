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
import java.text.MessageFormat


class ReplayMessages(val startDate:DateTime, val endDate:DateTime) {
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val magicString = "%^#"
  val threshold = 10000 // once we have narrowed down the desired date within 10k bytes, we look serially

  def parsedMessage(msg:JValue):MsgFormat= msg.extract[MsgFormat]


  def sequentialSearch(startPos:Long, raf:RandomAccessFile):Option[RandomAccessFile] ={
    None
  }

  /**
   * Given a date and a file, returns the position in the file which has a message with date >= given date
   * @param date
   * @param raf
   * @return
   */
  def seekDateTime(date:DateTime,raf:RandomAccessFile):Option[RandomAccessFile]=
  {
    val msg= getNextMsg(raf)
    if(msg.isDefined){
      val parsed= parsedMessage(msg.get)
      raf.seek(0)
      if(parsed.parsedDate>=date){ //if the very beginning of the file is already past our start timestamp
        Some(raf)
      }
      else{
        seekDateTime(0,raf.length(),date,raf)
      }
    }
    else{ // something's wrong with the file
      None
    }
  }


  // Find the position of the message that starts at the specified date, within the startPos and endPos boundaries
  def seekDateTime(startPos:Long,endPos:Long,date:DateTime,raf:RandomAccessFile):Option[RandomAccessFile]={
    if(endPos<=startPos) None // can't find the startdate
    else if(endPos-startPos <= threshold) sequentialSearch(startPos,raf)
    else{
      val midPos=(startPos+endPos)/2
      raf.seek(midPos)
      val msg = getNextMsg(raf)
      if(msg.isDefined){
        val parsed=parsedMessage(msg.get)
        if(parsed.parsedDate<date){
          seekDateTime(midPos,endPos,date,raf)
        }else if(parsed.parsedDate==date){
          Some(raf)
        }
        else{
          seekDateTime(startPos,midPos,date,raf)
        }
      }
      else None
    }
  }

  /**
   * Given a RandomAccessFile object, gets the next message from the file, starting from any arbitrary position.
   * Puts the RandomAccessFile pointer at the beginning of the line and returns the JSON value of that line
   * @param raf
   * @return
   */
  def getNextMsg(raf:RandomAccessFile):Option[JValue]={
    var currRafPos = raf.getFilePointer

    def parseLine(line:String):Option[JValue]=
      Some(parseJson(line.substring(magicString.length)))

    var line = raf.readLine()
    if(line.startsWith(magicString)){
      raf.seek(currRafPos) //This is where the line starts
      parseLine(line)
    }
    else{
      if(raf.getFilePointer < raf.length) { // within file boundary
        currRafPos=raf.getFilePointer
        line = raf.readLine()
        raf.seek(currRafPos) // put it back at the beginning of the line
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

package record

import java.io.Serializable

import scala.util.Try

/**
  * @author Manthan Thakar
  * @param values an array of string that will be converted into a record
  */
class ArtistTermRecord(values: Array[String]) extends Serializable with Record {
  val artistId: String = values(0)
  val artistTerm: String = values(1)
  val artistTermFreq: Float = convert(Try(values(2).toFloat), 0)
  val artistTermWeight: Float = convert(Try(values(3).toFloat), 0)

  override def toString: String = {
    "( " + artistId + ", " + artistTerm + ", " + artistTermFreq + ", " + artistTermWeight + " )"
  }
}
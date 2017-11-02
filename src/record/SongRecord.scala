package record

import java.io.Serializable

import scala.util.Try

/**
  * @author Manthan Thakar
  * @param values an array of string that will be converted into a record
  */
class SongRecord(values: Array[String]) extends Serializable with Record {
  val trackId: String = values(0)
  val audioMd5: String = values(1)
  val endOfFadeIn: Float = convert(Try(values(2).toFloat), 0)
  val startOfFadeOut: Float = convert(Try(values(3).toFloat), 0)
  val sampleRate: Float = convert(Try(values(4).toFloat), 0)
  val duration: Float = convert(Try(values(5).toFloat), 0)
  val loudness: Double = convert(Try(values(6).toDouble), 0)
  val tempo: Float = convert(Try(values(7).toFloat), 0)
  val key: Int = convert(Try(values(8).toInt), 0)
  val keyConfidence: Float = convert(Try(values(9).toFloat), 0)
  val mode: Int = convert(Try(values(10).toInt), 0)
  val modeConfidence: Float = convert(Try(values(11).toFloat), 0)
  val timeSignature: Int = convert(Try(values(12).toInt), 0)
  val timeSignatureConfidence: Float = convert(Try(values(13).toFloat), 0)
  val danceability: Float = convert(Try(values(14).toFloat), 0)
  val energy: Float = convert(Try(values(15).toFloat), 0)
  val artistId: String = values(16)
  val artistName: String = values(17)
  val artistLocation: String = values(18)
  val artistFamiliarity: Float = convert(Try(values(19).toFloat), 0)
  val artistHotness: Float = convert(Try(values(20).toFloat), 0)
  val genre: String = values(21)
  val release: String = values(22)
  val songId: String = values(23)
  val title: String = values(24)
  val songHotness: Float = convert(Try(values(25).toFloat), 0)
  val year: Int = convert(Try(values(26).toInt), 0)

  override def toString: String = {
    trackId + " " + title + " Artist:" + artistName + " Loudness:" + loudness
   }
}
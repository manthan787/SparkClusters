package record

import scala.util.{Failure, Success, Try}

/**
  * @author Manthan Thakar
  * This trait provides helpers to the record concrete classes
  */
trait Record {
  def convert[T](con: Try[T], fallback: T) = {
    con match {
      case Success(x) => x
      case Failure(e) => fallback
    }
  }
}

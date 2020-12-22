package scala

class LineElement(s:String) extends ArrayElement(Array(s)) {
  override def hight: Int = 1

  override def width: Int = s.length
}

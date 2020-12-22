package scala

class ArrayElement(val conts: Array[String]) extends Element {
  override def contents: Array[String] = conts

  override def toString: String = contents mkString "\n"


}

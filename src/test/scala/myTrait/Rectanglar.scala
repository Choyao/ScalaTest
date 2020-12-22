package scala.myTrait

trait Rectanglar {

  def bottomRight: Point

  def topLeft: Point

  def left = topLeft.x

  def right = bottomRight.x

  def width = right - left

}

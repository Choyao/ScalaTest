package scala.myTrait

class Rectangle(val topLeft:Point,val bottomRight:Point) extends Rectanglar {

}

object Rectangle{
  def main(args: Array[String]): Unit = {
    val rec = new Rectangle(new Point(1,1),new Point(10,10))

    print(rec.left +"  "+  rec.width + " " + rec.right)
  }
}
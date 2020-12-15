package scala

abstract class Element {
  def contents: Array[String]

  def hight: Int = contents.length

  def width: Int = if (hight == 0) 0 else contents(0).length

  def above(e: Element): Element = {
    val thisl = this widthen e.width
    val thatl = e widthen this.width
    Element.elem(thisl.contents ++ thatl.contents)
  }

  def beside(e: Element): Element = {
    val thish = this highten e.hight
    val thath = e highten this.hight
    new ArrayElement(
      for (
        (line1, line2) <- thish.contents zip thath.contents)
        yield line1 + line2
    )
  }

  def widthen(w: Int): Element =
    if (w <= width) this
    else {
      val left = Element.elem(' ', (w - width) / 2, hight)
      val right = Element.elem(' ', (w - width) / 2, hight)
      left beside this beside right
    }

  def highten(h: Int): Element =
    if (h <= hight) this
    else {
      val top = Element.elem(' ', width, (h - hight) / 2)
      val bot = Element.elem(' ', width, (h - hight) / 2)
      top above this above bot
    }
}

object Element {
  def elem(conts: Array[String]): Element = new ArrayElement(conts)

  def elem(conts: String): Element = new LineElement(conts)

  def elem(ch: Char, width: Int, hight: Int): Element = new UniformElement(ch, width, hight)

  def get(ar: Array[String], ar2: Array[String]): Array[String] = {
    for ((line, line2) <- ar zip ar2)
      yield line + line2
  }

  def main(args: Array[String]): Unit = {


    def longestWord(words: Array[String]) = {
      var word = words(0)
      var idx = 0
      for (i <- 1 until words.length)
        if (words(i).length > word.length) {
          word = words(i)
          idx = i
        }
      (word, idx)
    }


  }


}

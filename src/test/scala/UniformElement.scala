package scala

class UniformElement(
                      ch: Char,
                      override val width: Int,
                      override val hight: Int

                    ) extends Element {
  private val line = ch.toString * width

  override def contents: Array[String] = Array.fill(hight)(line)
}

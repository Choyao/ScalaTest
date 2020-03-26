import org.junit.{Assert, Test}

@Test
object ScalaTest extends Assert{
  @Test
  def echo(): Unit ={
    println("Hello Sister")
  }

  @Test
  def say(): Unit = {
    println("say what")
  }
}

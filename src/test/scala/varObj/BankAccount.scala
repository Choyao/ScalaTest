package scala.varObj

class BankAccount {
  private var bal : Int = 0

  def valance : Int = bal
  def deposit(amount:Int): Unit ={
    require(amount>0)
    bal +=amount
  }

  def withDraw(amount:Int):Boolean=
    if (amount>bal) false
    else {
      bal -= amount
      true
    }

}

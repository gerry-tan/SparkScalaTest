package sxt.core

/**
  * Created by root on 2016/8/9.
  */
class SecondSordKey(val first:Int,val second:Int) extends Ordered[SecondSordKey] with Serializable {
  /* Returns `x` where:
   *   - `x < 0` when `this < that`
   *   - `x == 0` when `this == that`
   *   - `x > 0` when  `this > that`
   */
  override def compare(that: SecondSordKey): Int = {
    if (this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}

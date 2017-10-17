/**
  * Created by Tan on 2017/8/2.
  */
object QuickSort {
  def quickSort(list: List[Int]): List[Int] = {
    list match {
      case Nil => Nil
      case List() => List()
      case head :: tail =>
        val (left, right) = tail.partition(_ < head) //以将数组
        quickSort(left) ::: head :: quickSort(right)
    }
  }

  def main(args: Array[String]) {
    val list = List(3, 12, 43, 23, 7, 1, 2, 0)
    println("End: "+quickSort(list))
  }
//  def main(args: Array[String]): Unit = {
//    val arr:Array[Int]= Array(23,42,12,63,73,24,15)
//    sortModel(arr,0,arr.length-1)
//    println("ok")
//  }
//
//  def sortModel(arr: Array[Int], l: Int, r: Int) {
//    if(l<r){
//      var i=l
//      var j=r
//      var temp = arr(l)
//      while(i<j){
//        while(i<j && arr(j)>temp) j=j-1
//        if (i<j) arr(i+1)=arr(j)
//        while(i<j && arr(i)<temp) i+i+1
//        if (i<j) arr(j-1)=arr(i)
//      }
//      arr(i)=temp
//      sortModel(arr,l,j-1)
//      sortModel(arr,i+1,r)
//    }
//  }
}



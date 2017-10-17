//package sxt.Streaming
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase._
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.filter.PrefixFilter
//
//import scala.sys.process.processInternal.IOException
//
///**
//  * Created by Tan on 2016/10/17.
//  */
//object HBaseDAOImp {
//  private[Streaming] var conf: Configuration = null
//
//  def main(args: Array[String]) {
//    val dao: HBaseDAO = new HBaseDAOImp
//    val list: List[Result] = dao.getRows("test", "testrow", Array[String]("age"))
//    import scala.collection.JavaConversions._
//    for (rs <- list) {
//      for (cell <- rs.rawCells) {
//        System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ")
//        System.out.println("Timetamp:" + cell.getTimestamp + " ")
//        System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ")
//        System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ")
//        System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ")
//      }
//    }
//    val rs: Result = dao.getOneRow("test", "testrow")
//    System.out.println(new String(rs.getValue("cf".getBytes, "age".getBytes)))
//  }
//}
//
//class HBaseDAOImp extends HBaseDAO {
//  private[Streaming] var hTablePool: HConnection = null
//
//  def this() {
//    this()
//    HBaseDAOImp.conf = new Configuration
//    val zk_list: String = "spark001:2181,spark002:2181,spark003:2181"
//    HBaseDAOImp.conf.set("hbase.zookeeper.quorum", zk_list)
//    try {
//      hTablePool = HConnectionManager.createConnection(HBaseDAOImp.conf)
//    }
//    catch {
//      case e: IOException => {
//        e.printStackTrace
//      }
//    }
//  }
//
//  def save(put: Put, tableName: String) {
//    var table: HTableInterface = null
//    try {
//      table = hTablePool.getTable(tableName)
//      table.put(put)
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//  }
//
//  def insert(tableName: String, rowKey: String, family: String, quailifer: String, value: String) {
//    var table: HTableInterface = null
//    try {
//      table = hTablePool.getTable(tableName)
//      val put: Put = new Put(rowKey.getBytes)
//      put.add(family.getBytes, quailifer.getBytes, value.getBytes)
//      table.put(put)
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//  }
//
//  def insert(tableName: String, rowKey: String, family: String, quailifer: List[String], value: List[String]) {
//    var table: HTableInterface = null
//    try {
//      table = hTablePool.getTable(tableName)
//      val put: Put = new Put(rowKey.getBytes)
//      {
//        var i: Int = 0
//        while (i < quailifer.length) {
//          {
//            val col: String = quailifer(i)
//            val `val`: String = value(i)
//            put.add(family.getBytes, col.getBytes, `val`.getBytes)
//          }
//          ({
//            i += 1; i - 1
//          })
//        }
//      }
//      table.put(put)
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//  }
//
//  def save(Put: List[Put], tableName: String) {
//    var table: HTableInterface = null
//    try {
//      table = hTablePool.getTable(tableName)
//      table.put(Put)
//    }
//    catch {
//      case e: Exception => {
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//  }
//
//  def getOneRow(tableName: String, rowKey: String): Result = {
//    var table: HTableInterface = null
//    var rsResult: Result = null
//    try {
//      table = hTablePool.getTable(tableName)
//      val get: Get = new Get(rowKey.getBytes)
//      rsResult = table.get(get)
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//    return rsResult
//  }
//
//  def getRows(tableName: String, rowKeyLike: String): List[Result] = {
//    var table: HTableInterface = _
//    var list: List[Result] = _
//    try
//      table = hTablePool.getTable(tableName)
//      val filter: PrefixFilter = new PrefixFilter(rowKeyLike.getBytes)
//      val scan: Scan = new Scan
//      scan.setFilter(filter)
//      val scanner: ResultScanner = table.getScanner(scan)
//      list = new util.ArrayList[Result]
//      import scala.collection.JavaConversions._
//      for (rs <- scanner) {
//        list.add(rs)
//      }
//
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//    return list
//  }
//
//  def getRows(tableName: String, rowKeyLike: String, cols: String): List[Result] = {
//    var table: HTableInterface = null
//    var list: List[Result] = null
//    try {
//      table = hTablePool.getTable(tableName)
//      val filter: PrefixFilter = new PrefixFilter(rowKeyLike.getBytes)
//      val scan: Scan = new Scan
//      {
//        var i: Int = 0
//        while (i < cols.length) {
//          {
//            scan.addColumn("cf".getBytes, cols(i).getBytes)
//          }
//          ({
//            i += 1; i - 1
//          })
//        }
//      }
//      scan.setFilter(filter)
//      val scanner: ResultScanner = table.getScanner(scan)
//      list = new util.ArrayList[Result]
//      import scala.collection.JavaConversions._
//      for (rs <- scanner) {
//        list.add(rs)
//      }
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//    return list
//  }
//
//  def getRows(tableName: String, startRow: String, stopRow: String): List[Result] = {
//    var table: HTableInterface = null
//    var list: List[Result] = null
//    try {
//      table = hTablePool.getTable(tableName)
//      val scan: Scan = new Scan
//      scan.setStartRow(startRow.getBytes)
//      scan.setStopRow(stopRow.getBytes)
//      val scanner: ResultScanner = table.getScanner(scan)
//      list = new util.ArrayList[Result]
//      import scala.collection.JavaConversions._
//      for (rsResult <- scanner) {
//        list.add(rsResult)
//      }
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//    return list
//  }
//
//  def deleteRecords(tableName: String, rowKeyLike: String) {
//    var table: HTableInterface = null
//    try {
//      table = hTablePool.getTable(tableName)
//      val filter: PrefixFilter = new PrefixFilter(rowKeyLike.getBytes)
//      val scan: Scan = new Scan
//      scan.setFilter(filter)
//      val scanner: ResultScanner = table.getScanner(scan)
//      val list: List[Delete] = new util.ArrayList[Delete]
//      import scala.collection.JavaConversions._
//      for (rs <- scanner) {
//        val del: Delete = new Delete(rs.getRow)
//        list.add(del)
//      }
//      table.delete(list)
//    }
//    catch {
//      case e: Exception => {
//        e.printStackTrace
//      }
//    } finally {
//      try {
//        table.close
//      }
//      catch {
//        case e: IOException => {
//          e.printStackTrace
//        }
//      }
//    }
//  }
//
//  def createTable(tableName: String, columnFamilys: Array[String]) {
//    try {
//      val admin: HBaseAdmin = new HBaseAdmin(HBaseDAOImp.conf)
//      if (admin.tableExists(tableName)) {
//        System.err.println("此表，已存在！")
//      }
//      else {
//        val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
//        for (columnFamily <- columnFamilys) {
//          tableDesc.addFamily(new HColumnDescriptor(columnFamily))
//        }
//        admin.createTable(tableDesc)
//        System.err.println("建表成功!")
//      }
//      admin.close
//    }
//    catch {
//      case e: MasterNotRunningException => {
//        e.printStackTrace
//      }
//      case e: ZooKeeperConnectionException => {
//        e.printStackTrace
//      }
//      case e: IOException => {
//        e.printStackTrace
//      }
//    }
//  }
//
//  /**
//    * 删除一个表
//    *
//    * @param tableName
//    * 删除的表名
//    **/
//  def deleteTable(tableName: String) {
//    try {
//      val admin: HBaseAdmin = new HBaseAdmin(HBaseDAOImp.conf)
//      if (admin.tableExists(tableName)) {
//        admin.disableTable(tableName)
//        admin.deleteTable(tableName)
//        System.err.println("删除表成功!")
//      }
//      else {
//        System.err.println("删除的表不存在！")
//      }
//      admin.close
//    }
//    catch {
//      case e: MasterNotRunningException => {
//        e.printStackTrace
//      }
//      case e: ZooKeeperConnectionException => {
//        e.printStackTrace
//      }
//      case e: IOException => {
//        e.printStackTrace
//      }
//    }
//  }
//
//  /**
//    * 查询表中所有行
//    *
//    * @param tablename
//    */
//  def scaner(tablename: String) {
//    try {
//      val table: HTable = new HTable(HBaseDAOImp.conf, tablename)
//      val s: Scan = new Scan
//      val rs: ResultScanner = table.getScanner(s)
//      import scala.collection.JavaConversions._
//      for (r <- rs) {
//        val kv: Array[KeyValue] = r.raw
//        {
//          var i: Int = 0
//          while (i < kv.length) {
//            {
//              System.out.print(new String(kv(i).getRow) + "")
//              System.out.print(new String(kv(i).getFamily) + ":")
//              System.out.print(new String(kv(i).getQualifier) + "")
//              System.out.print(kv(i).getTimestamp + "")
//              System.out.println(new String(kv(i).getValue))
//            }
//            ({
//              i += 1; i - 1
//            })
//          }
//        }
//      }
//    }
//    catch {
//      case e: IOException => {
//        e.printStackTrace
//      }
//    }
//  }
//}

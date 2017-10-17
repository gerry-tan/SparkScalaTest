package sxt.Streaming

import org.apache.hadoop.hbase.client.{Result, Put}


trait HBaseDAO {
  def save(put: Put, tableName: String)

  def insert(tableName: String, rowKey: String, family: String, quailifer: String, value: String)

//  def insert(tableName: String, rowKey: String, family: String, quailifer: String, value: String)

  def save(Put: List[Put], tableName: String)

  def getOneRow(tableName: String, rowKey: String): Result

  def getRows(tableName: String, rowKey_like: String): List[Result]

  def getRows(tableName: String, rowKeyLike: String, cols: String): List[Result]

//  def getRows(tableName: String, startRow: String, stopRow: String): List[Result]

  def deleteRecords(tableName: String, rowKeyLike: String)

  def deleteTable(tableName: String)

  def createTable(tableName: String, columnFamilys: Array[String])
}

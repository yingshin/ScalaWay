import org.apache.calcite.sql.{SqlJoin, SqlSelect}
import org.apache.calcite.sql.parser.SqlParser

import scala.collection.JavaConverters._

/**
 * Author: zhangying14
 * Date: 2020/9/23 21:11
 * Package: 
 * Description:
 *
 */
object CalciteSqlParserSample extends App {
  val parser = SqlParser.create(
    """
      |SELECT a.id, a.name, b.exam_name FROM csv_table As a JOIN tblDimExam as b ON (a.id = b.exam_id) WHERE id > 0
      |""".stripMargin)
//  FOR SYSTEM_TIME AS OF a.proctime
  val sqlNode = parser.parseQuery()
  println(sqlNode.getClass)

  val selectSqlNode = sqlNode.asInstanceOf[SqlSelect]
  selectSqlNode.getSelectList.getList.asScala.zipWithIndex.foreach{case (sqlNode, index) =>
    println(s"index:${index} node:${sqlNode}")
  }

  val from = selectSqlNode.getFrom
  from match {
    case sqlJoin: SqlJoin => {
      println(s"left:  ${sqlJoin.getLeft}")
      println(s"right: ${sqlJoin.getRight}")
      println(s"join:  ${sqlJoin.getJoinType}")
      println(s"cond:  ${sqlJoin.getCondition}")
      println(s"oper:  ${sqlJoin.getOperator}")
      println(s"operL: ${sqlJoin.getOperandList.asScala}")
    }
  }

//  println(s"from:  ${selectSqlNode.getFrom}")
  println(s"fetch: ${selectSqlNode.getFetch}")
  println(s"where: ${selectSqlNode.getWhere}")

}

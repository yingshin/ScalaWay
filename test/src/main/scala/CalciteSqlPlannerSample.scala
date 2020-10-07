import java.util.Properties

import org.apache.calcite.adapter.java.ReflectiveSchema
import org.apache.calcite.config.CalciteConnectionConfigImpl
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.{HepPlanner, HepProgramBuilder}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeSystem, RelDataTypeSystemImpl}
import org.apache.calcite.rel.rules.FilterJoinRule
import org.apache.calcite.schema.impl
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeFactoryImpl, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Planner}

/**
 * Author: zhangying14
 * Date: 2020/9/24 20:43
 * Package: 
 * Description:
 *
 */
object CalciteSqlPlannerSample extends App {
  val schema = Frameworks.createRootSchema(true)
  schema.add("USERS", new AbstractTable {
    override def getRowType(relDataTypeFactory: RelDataTypeFactory): RelDataType = {
      val builder = relDataTypeFactory.builder()

      builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl {}, SqlTypeName.INTEGER))
      builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl {}, SqlTypeName.CHAR))
      builder.add("AGE", new BasicSqlType(new RelDataTypeSystemImpl {}, SqlTypeName.INTEGER))

      builder.build()
    }
  })

  schema.add("JOBS", new impl.AbstractTable {
    override def getRowType(relDataTypeFactory: RelDataTypeFactory): RelDataType = {
      val builder = relDataTypeFactory.builder()

      builder.add("ID", new BasicSqlType(new RelDataTypeSystemImpl {}, SqlTypeName.INTEGER))
      builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl {}, SqlTypeName.CHAR))
      builder.add("COMPANY", new BasicSqlType(new RelDataTypeSystemImpl {},SqlTypeName.CHAR))

      builder.build()
    }
  })

  val sql =
    """
      |select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age
      |from users u join jobs j on u.name=j.name
      |where u.age > 30 and j.id>10
      |order by user_id
      |""".stripMargin

 def way1: Unit = {
   val sqlParser = SqlParser.create(sql, SqlParser.Config.DEFAULT)

   // parser: sql -> sqlNode
   val sqlNode = sqlParser.parseStmt()
   println(sqlNode)

   // validate: sqlNode -> sqlNode
   val sqlTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
   val calciteCatalogReader = new CalciteCatalogReader(
     CalciteSchema.from(schema),
     CalciteSchema.from(schema).path(null),
     sqlTypeFactory,
     new CalciteConnectionConfigImpl(new Properties()))

   val configBuilder = Frameworks.newConfigBuilder
   //设置默认schema
   configBuilder.defaultSchema(schema)
   val frameworkConfig = configBuilder.build

   val sqlValidator = SqlValidatorUtil.newValidator(
     SqlStdOperatorTable.instance(),
     calciteCatalogReader,
     sqlTypeFactory)

   val validateSqlNode = sqlValidator.validate(sqlNode)
   println(validateSqlNode)
 }

  def way2: Unit = {
    val configBuilder = Frameworks.newConfigBuilder
    //设置默认 schema
    configBuilder.defaultSchema(schema)

    val frameworkConfig = configBuilder.build
    val parserConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig)

    //SQL 大小写不敏感
    parserConfig.setCaseSensitive(false).setConfig(parserConfig.build)
    val planner = Frameworks.getPlanner(frameworkConfig)

    val sqlNode = planner.parse(sql)
    planner.validate(sqlNode)
    val relRoot = planner.rel(sqlNode)
    val relNode = relRoot.project()
    println(s"relNode toString:\n${RelOptUtil.toString(relNode)}")

    val hepProgramBuilder = new HepProgramBuilder
    hepProgramBuilder.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
    val hepPlanner = new HepPlanner(hepProgramBuilder.build())
    hepPlanner.setRoot(relNode)
    println(s"findBestExp:${hepPlanner.findBestExp()}")
    println(s"toString:\n${RelOptUtil.toString(hepPlanner.findBestExp())}")
  }

  way2
}

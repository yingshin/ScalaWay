import java.io.File

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import scala.collection.JavaConverters._

/**
 * Author: zhangying14
 * Date: 2020/9/24 10:30
 * Package: 
 * Description:
 *
 */
object JacksonCSVSample extends App {
  val mapper = new CsvMapper
  val csvSchema = CsvSchema.builder()
    .addColumn("firstName")
    .addColumn("lastName")
    .addColumn("age", CsvSchema.ColumnType.NUMBER)
    .build()

  val iter = mapper.readerFor(classOf[java.util.Map[String, Any]])
    .`with`(csvSchema)
    .readValues(new File("/tmp/csv.data"))

  while (iter.hasNext) {
    println(iter.next())
  }
}

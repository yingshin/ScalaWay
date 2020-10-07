//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, MappingIterator}
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema.Column
import com.fasterxml.jackson.databind.{JsonNode, MappingIterator}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Column


/**
 * Author: zhangying14
 * Date: 2020/9/14 21:53
 * Package: 
 * Description:
 *
 */
object CsvMapperSample extends App {
  val builder = new CsvSchema.Builder()
  builder.addColumn(new Column(0, "c1", CsvSchema.ColumnType.ARRAY))
  builder.addColumn(new Column(1, "c2", CsvSchema.ColumnType.NUMBER))

  val csvSchema = builder.build().rebuild().build()

  val iterator: MappingIterator[JsonNode] = new CsvMapper()
    .readerFor(classOf[JsonNode])
    .`with`(csvSchema)
      .readValues(",123")
//    .readValues("/tmp/csv.sink/part-aff33558-fb06-42a5-8a2a-fbf5fcc1f00d-0-0")

  println(iterator.nextValue())

}

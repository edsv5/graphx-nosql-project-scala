import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import spark.implicits._

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files
val path = "/home/daniel/Downloads/Carreteras_de_Costa_Rica.geojson"

val schema = (new StructType)
  .add("features", (new ArrayType)
    .add("element", (new StructType)
      .add("geometry", (new StructType)
      	.add("coordinates", (new ArrayType)
      	  .add("element", (new ArrayType)
      	    .add("element", StringType)
          )
        )
      )
      .add("properties", (new StructType)
        .add("NRUTA", StringType)
        .add("OBJECTID", LongType)
	.add("OBJECTID_1", LongType)
        .add("RL", StringType)
	.add("RUTA", LongType)
        .add("SHAPE_Leng", DoubleType)
	.add("TIPO", StringType)
      )
      .add("event", StringType)
    )
  )
  .add("event", StringType)

//val peopleDF = spark.read.json(path)
val peopleDF = spark.read.schema(schema).json(path)

// The inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()

//spark.read.schema(schema).json(path).select($"features.element.properties.*").show

/*val schema = (new StructType)
  .add("payload", (new StructType)
    .add("event", (new StructType)
      .add("action", StringType)
      .add("timestamp", LongType)
    )
  )
*/


/*sqlContext.read.schema(schema).json(df).select($"payload.event.*").show*/



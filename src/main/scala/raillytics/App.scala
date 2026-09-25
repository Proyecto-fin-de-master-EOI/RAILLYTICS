package raillytics

// Main por defecto del proyecto sbt (`sbt run`): solo identifica el artefacto.
// Las apps reales se lanzan con runMain (ver Makefile): RawUploaderApp (L1),
// ParquetConverterApp (L2) y GoldBuilderApp (Gold).
object App {
  def main(args: Array[String]): Unit = {
    println("raillytics-spark-jobs")
  }
}

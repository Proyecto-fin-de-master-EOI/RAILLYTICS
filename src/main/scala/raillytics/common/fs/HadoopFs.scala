package raillytics.common.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import java.io.IOException
import java.net.URI

// Ayudas sobre la API FileSystem de Hadoop, la que usan L1 y L2 para copiar y
// mover ficheros: la misma llamada sirve para el disco local (file://) y para
// MinIO (s3a://), según el esquema de la ruta.
object HadoopFs {

  // FileSystem del disco local, con la configuración Hadoop de la SparkSession
  // (en Windows necesita winutils.exe: HADOOP_HOME, ver .env.example).
  def local(hadoopConf: Configuration): FileSystem =
    FileSystem.get(new URI("file:///"), hadoopConf)

  // FileSystem que corresponde al esquema de root: s3a://bucket -> MinIO (con
  // las credenciales que SparkSessionFactory dejó en hadoopConf), file:// -> local.
  def forRoot(root: String, hadoopConf: Configuration): FileSystem =
    FileSystem.get(new URI(root), hadoopConf)

  // Mueve srcPath dentro de destDir (creándolo si no existe) con el nombre fileName
  // y devuelve la ruta destino. En disco local rename es atómico: el fichero nunca
  // está "a medias" en destino. rename() devuelve false en vez de lanzar cuando no
  // puede (destino ya existente, origen desaparecido): se convierte en excepción
  // para que un fichero que no se ha movido no pase desapercibido (volvería a
  // entrar en el siguiente glob o se quedaría huérfano en silencio).
  def moveInto(fs: FileSystem, srcPath: Path, destDir: Path, fileName: String): Path = {
    fs.mkdirs(destDir)
    val dest = new Path(destDir, fileName)
    if (!fs.rename(srcPath, dest)) {
      throw new IOException(s"no se pudo mover '$srcPath' a '$dest' (¿existe ya el destino o ha desaparecido el origen?)")
    }
    dest
  }

  // Igual que la anterior conservando el nombre original del fichero.
  def moveInto(fs: FileSystem, srcPath: Path, destDir: Path): Path =
    moveInto(fs, srcPath, destDir, srcPath.getName)

  // Escribe df como UN unico fichero <nombre>.parquet dentro de dir.
  //
  // No se usa `write.mode("append").parquet(dir)`: el committer de Hadoop monta
  // TODAS las escrituras de un mismo directorio en <dir>/_temporary y lo borra al
  // terminar, asi que dos escrituras concurrentes se pisan ahi y se pierden
  // ficheros. Y la concurrencia es la norma, no la excepcion: L2 lanza una query
  // por fuente sobre la misma SparkSession, y ademas L1 y L2 corren en procesos
  // distintos (el README pide una terminal para cada uno) escribiendo al mismo
  // prefijo de trazabilidad.
  //
  // Dandole a cada escritura un staging propio, el committer ya no comparte nada.
  // El resultado es el mismo convenio de un fichero por ejecucion que sigue el lado
  // Python con DuckDB (LakeLayout.cargas_file en python/raillytics/utils/lake.py).
  def escribirFicheroUnico(df: DataFrame, dir: String, nombre: String): Unit = {
    val staging = new Path(s"$dir/_staging-$nombre")
    val fs = staging.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    try {
      df.coalesce(1).write.mode("overwrite").parquet(staging.toString)
      val part = fs.listStatus(staging).map(_.getPath).find(_.getName.endsWith(".parquet"))
        .getOrElse(throw new IOException(s"no se genero ningun Parquet en el staging '$staging'"))
      moveInto(fs, part, new Path(dir), s"$nombre.parquet")
    } finally fs.delete(staging, true)
  }
}

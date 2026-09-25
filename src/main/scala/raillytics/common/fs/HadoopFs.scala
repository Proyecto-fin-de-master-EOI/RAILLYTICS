package raillytics.common.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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

  // Mueve srcPath dentro de destDir (creándolo si no existe) con el nombre fileName.
  // En disco local rename es atómico: el fichero nunca está "a medias" en destino.
  def moveInto(fs: FileSystem, srcPath: Path, destDir: Path, fileName: String): Unit = {
    fs.mkdirs(destDir)
    fs.rename(srcPath, new Path(destDir, fileName))
  }

  // Igual que la anterior conservando el nombre original del fichero.
  def moveInto(fs: FileSystem, srcPath: Path, destDir: Path): Unit =
    moveInto(fs, srcPath, destDir, srcPath.getName)
}

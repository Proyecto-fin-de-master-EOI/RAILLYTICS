package raillytics.common.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

object HadoopFs {

  def local(hadoopConf: Configuration): FileSystem =
    FileSystem.get(new URI("file:///"), hadoopConf)

  def forRoot(root: String, hadoopConf: Configuration): FileSystem =
    FileSystem.get(new URI(root), hadoopConf)

  // Mueve srcPath dentro de destDir (creándolo si no existe) con el nombre fileName.
  def moveInto(fs: FileSystem, srcPath: Path, destDir: Path, fileName: String): Unit = {
    fs.mkdirs(destDir)
    fs.rename(srcPath, new Path(destDir, fileName))
  }

  def moveInto(fs: FileSystem, srcPath: Path, destDir: Path): Unit =
    moveInto(fs, srcPath, destDir, srcPath.getName)
}

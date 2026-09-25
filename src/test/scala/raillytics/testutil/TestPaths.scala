package raillytics.testutil

import java.nio.file.Path

object TestPaths {
  // URI file:// válida en cualquier SO. Concatenar s"file://$path" solo funciona
  // en Linux (file:///tmp/...); en Windows da file://C:\... y Hadoop interpreta
  // "C:" como authority -> "Wrong FS" / URISyntaxException. Sin "/" final para
  // que BronzePaths no genere dobles barras.
  def fileUri(path: Path): String = path.toUri.toString.stripSuffix("/")
}

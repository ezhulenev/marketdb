package integration.ergodicity.marketdb

import java.io.File
import com.twitter.logging.Logger

trait EvalSupport {
  val configTarget: Option[String] = Some("target")

  protected def getConfigTarget(configFile: File): Option[File] = {
    configTarget flatMap {
      fileName =>
      // if we have a config file, try to make the target dir a subdirectory
      // of the directory the config file lives in (e.g. <my-app>/config/target)
        val targetFile = if (configFile.exists && configFile.getParentFile != null) {
          new File(configFile.getParentFile, fileName)
        } else {
          new File(fileName)
        }

        // make sure we can get an actual directory, otherwise fail and return None
        if (!targetFile.exists) {
          if (targetFile.mkdirs()) {
            Some(targetFile)
          } else {
            Logger.get("").warning("couldn't make directory %s. will not cache configs", targetFile)
            None
          }
        } else if (!targetFile.isDirectory) {
          throw new IllegalArgumentException("specified target directory %s exists and is not a directory".
            format(fileName))
          None
        } else {
          Some(targetFile)
        }
    }
  }


}
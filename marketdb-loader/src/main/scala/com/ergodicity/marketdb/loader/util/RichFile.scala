package com.ergodicity.marketdb.loader.util

import java.io.{FileWriter, BufferedReader, FileReader, File}


class RichFile(file: File) {
  def write(text: String): Unit = {
    val fw = new FileWriter(file)
    try {
      fw.write(text)
    }
    finally {
      fw.close
    }
  }

  def foreachLine(proc: String => Unit): Unit = {
    val br = new BufferedReader(new FileReader(file))
    try {
      while (br.ready) proc(br.readLine)
    }
    finally {
      br.close
    }
  }

  def deleteAll: Boolean = {
    def deleteFile(dfile: File): Boolean = {
      if (dfile.isDirectory) {
        val subfiles = dfile.listFiles
        if (subfiles != null)
          subfiles.foreach {
            f => deleteFile(f)
          }
      }
      dfile.delete
    }
    deleteFile(file)
  }
}

object RichFile {
  implicit def file2helper(file: File) = new RichFile(file)
}

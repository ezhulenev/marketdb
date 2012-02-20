package com.ergodicity.marketdb.loader

import tools.nsc.io.File

sealed abstract class DataStorage

case class LocalStorage(root: File, pattern: String) extends DataStorage {
  if (!root.exists || !root.isDirectory) throw new IllegalArgumentException("Local storage root directory doesn't exists")
}

case class RemoteStorage(base: String, pattern: String) extends DataStorage {
  if (base == null || base.isEmpty) throw new IllegalArgumentException("Remote storage base URL should be defined")
}


sealed abstract class DataChunk

case class ZipFileChunk
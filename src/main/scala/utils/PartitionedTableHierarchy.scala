package utils

import java.io.File

object PartitionedTableHierarchy {

  /*
  To clean up the output table from the last run
  */
  def delteRecursively(file: File): Unit = {
    if(file.isDirectory)
      file.listFiles.foreach(delteRecursively)
  }


  /*
  Count the number of generated files with the given suffix
  */
  def countRecursively(file: File, suffix: String): Int={
    if(file.isDirectory){
      val counts = file.listFiles.map(f => countRecursively(f, suffix))
      counts.toList.sum
    } else {
      if (file.getName.endsWith(suffix)) 1 else 0
    }
  }


  //Print the directory Hierarchy
  def printRecursively(file: File, indent: Int=0): Unit={
    0.to(indent).foreach(i => print(" "))
    if (file.isDirectory){
      print("Directory: "+file.getName)
      file.listFiles.foreach(f => printRecursively(f, indent+1))
    } else {
      println("File: "+file.getName)
    }
  }








  }
}

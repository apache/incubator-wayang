package org.qcri.rheem.apps.simwords

/**
  * Scrubs texts.
  */
class TextScrubber {

  def splitAndScrub(line: String, collector: java.util.List[String]): Unit = {
    for (token <- line.split("""\W""") if !token.isEmpty) {
      collector.add(token.toLowerCase)
    }
  }

}

The Hadoop Distributed RDF Store (HDRS) is a scalable distributed peer-to-peer storage infrastructure for web-scale RDF data sets.  Its goal is to facilitate RDF analysis.  HDRS can be used as a data sink and source for Hadoop-Map/Reduce jobs.


---


**The HDRS source code is now available in the repository (trunk)**

**We are working on several improvements, including documentation, before releasing version 0.1**


---


## HDRS Architecture Overview ##

<a href='http://hdrs.googlecode.com/files/hdrs_architecture.png'><img src='http://hdrs.googlecode.com/files/hdrs_architecture.png' width='300' /></a>

(click on image for large version)

## Installation, Configuration, etc. ##
Please refer to [README.txt](https://code.google.com/p/hdrs/source/browse/trunk/README.txt) in the repository.

## Reading/Writing triples from/to a HDRS store ##
  * [This example](https://code.google.com/p/hdrs/source/browse/trunk/src/main/java/de/hpi/fgis/hdrs/client/Example.java) shows how to read/write Triples using the [HDRS Client API](https://code.google.com/p/hdrs/source/browse/trunk/src/main/java/de/hpi/fgis/hdrs/client/Client.java).
  * [This page](https://code.google.com/p/hdrs/wiki/HDRSLoader) explains how to batch-load triples using the [HDRS Loader](https://code.google.com/p/hdrs/source/browse/trunk/src/main/java/de/hpi/fgis/hdrs/tools/Loader.java).

## Documents and Publications ##
  * [Master's thesis on HDRS](http://hdrs.googlecode.com/files/hrds_thesis.pdf)
  * [Presentation on HDRS](http://hdrs.googlecode.com/files/hdrs_slides.pdf)
  * [I-SEMANTICS](http://i-semantics.tugraz.at/) paper on HDRS (coming soon)
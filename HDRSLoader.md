# HDRS Loader #
The HDRS Loader is a util for batch-loading triples into a HDRS store.  The Loader takes flat files of triples in the canonical NTriples format as input.  The files can be GZIPed.

First install HDRS on the machine you want to run the batch-load from as described in [README.txt](https://code.google.com/p/hdrs/source/browse/trunk/README.txt). (This machine doesn't have to be part of your HDRS store instance.)

Now lets assume you want to load the Billion Triple Challenge data set (BTC2011) into your HDRS store (the data set can be downloaded here: http://challenge.semanticweb.org/).  Place all files into one directory which we call `$BTC`.

To start the batch load, go to the HDRS home directory and run:
`bin/load.sh StoreAddress:Port -dz $BTC`

Explanation: `d` instructs the Loader to load all files in the $BTC directory, `z` instructs the Loader to decompress the files to be loaded using GZIP.

This will load the BTC data set file-by-file into the HDRS store.  Note you can run several Loaders in parallel (possibly on different machines) to increase the bandwidth.
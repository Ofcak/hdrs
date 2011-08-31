
=========================================
   HDRS: Hadoop Dristributed RDF Store
=========================================


PREREQUISITES

 * Currently, HDRS can only be run on UNIX-like systems (Linux, MacOSX, ...)
   since it relies on some shell scripts.
   Windows is currently not supported; HDRS was NOT tested with cygwin.
   Nevertheless, Windows can be used for HDRS development using Eclipse.

 * HDRS must be build from source; for this, you need Maven (2.x or 3.x) 
   and Java JDK 1.6.
   
 * HDRS uses Snappy compression (http://code.google.com/p/snappy/)
   Snappy requires libstdc++ version GLIBCXX_3.4.9 or newer.
   (You can query the version installed on your system using rpm:
    for this, simply run "rpm -q --provides libstdc++")
 

INSTALLATION

  1.  Copy the HDRS source into some directory (which we call $HDRS_HOME),
      e.g. by checking out the project SVN repository located at 
      http://code.google.com/p/hdrs/source/checkout
  
  2.  cd to $HDRS_HOME
  
  3.  run "mvn install"
  
  4.  Repeat steps 1-3 for all machines that are supposed to run HDRS.
  
 
CONFIGURATION

  1.  Assign a unique NodeID to each node.
      Write each node's NodeID to the $HDRS_HOME/conf/node file.
      
  2.  Configure nodes in $HDRS_HOME/conf/peers
      For each node in the store there is one line in the following format:
      
      <NodeID><tab><NodeAddress><tab><Port>
      
      Copy the file onto each node.
      The file MUST be equal on all nodes! (this includes the order of lines!)
      
  3.  Configure the indexes in $HDRS_HOME/conf/indexes
      For each index (in SPO, SOP, PSO, POS, OSP, OPS) add one line
      with the index name.
      
  4.  Configure the max heap space (-Xmx<HeapSpaceInMB>m) in $HDRS_HOME/bin/node.sh
      The more heap space, the better, but don't assign more space than is
      physically present in the machine, minus some space for the OS.
      
  5.  Configure the node settings in $HDRS_HOME/conf/hdrs-site.xml
  
      There are numerous options, see $HDRS_HOME/src/main/resources/hdrs-default.xml
      for available options and default settings.
      
      The most important option is "hdrs.rootdir", which defines where a HDRS
      node stores the triple data on disk.  The default for this is the tmp-
      directory, which should be changed to something else.
      
      Another important option is the RPC handler count "hdrs.rpc.handler.count".
      This should be set to 2 * number of nodes (roughly).
      
      
STARTING HDRS

  1.  On each node, cd to $HDRS_HOME
    
  2.  Run bin/start-node.sh
  
      The node will print log information to stderr.
      It is recommended to redirect this to some log file, 
      e.g. by running ". start-node.sh 2> node.out &"
      You need to start all nodes of a HDRS store for the store to be
      operational.
      

STOPPING HDRS

  1.  On each node, cd to $HDRS_HOME
    
  2.  Run bin/stop-node.sh
  
  OR
  
  1.  Start the HDRS Shell
  
  2.  Run "all shutdown"
  

RUN THE HDRS SHELL

  * The HDRS shell is a remote management util for HDRS.
  
  1.  Install HDRS like described in "INSTALLATION"
      (on some machine you want to run the shell on, e.g. your workstation)
      
  2.  cd to $HDRS_HOME
    
  3.  Run bin/shell.sh
  
      The shell will prompt you for the address of an arbitrary node of
      a HDRS store to connect to.
      

SETUP ECLIPSE FOR HDRS DEVELOPMENT

  1.  Install HDRS like described in "INSTALLATION"
      (on your development machine with Eclipse installed)
      
  2.  Run "mvn eclipse:eclipse"
  
  3.  Open Eclipse, File->New->Java Project,
      "Create project from existing source", select $HDRS_HOME
      
      

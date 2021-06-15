# approximating_laser

Both the LASER prototype we use and our queries were developed in Python.
As such, these should be ran with python2 to avoid dependencies problems.

How to run?
First download the file laser.zip, unzip it, and enter the respective folder. 
To obtain the results of a query, just run the file qX.py, where X is the number of chosen query. 
The respective output will be saved in the folder "qX_output".


The python scripts will generate an output file with LASER's output stream.
To change the default output folder, the variable "laser_output" should be changed to an existing directory.


In the function create_stream, the commented code provides useful statistics about LASER's input stream.
The scripts also print useful information about LASER's processing time.


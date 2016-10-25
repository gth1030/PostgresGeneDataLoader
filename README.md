# Bedfile uploader for PostgreSQL Chado Schema.

# How to run
Program can be built from build.sh and run by bedfile-loader.sh
Json file with necessary information must be fed to support upload of each data file.
Json file will contain meta data for dat files.
configuration.config file must be fed or updated to run program.

# Program basics
The program provides quick and easy upload to PostgreSQL with Chado Schema. General data type supported by the prgoram is .bed format.
First line of the file is first line of data. Each column is separated by tab. By default, program mock commits data on the database for testing. By feeding extra argument, program will commit data without rolling them back. The program runs series of sanity checks to prevent incorrect uploads. All of cvproperty, type, and scrfeature must be mapped in the database before hand. 

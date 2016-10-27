# Bedfile uploader for PostgreSQL Chado Schema.

# How to run
Program can be built from build.sh and run by bedfile-loader.sh
Json file with necessary information must be fed to support upload of each data file.
Json file will contain meta data for dat files.
configuration.config file must be fed or updated to run program.

# Program basics
The program provides quick and easy upload to PostgreSQL with Chado Schema. General data type supported by the prgoram is .bed format.
First line of the file is first line of data. Each column is separated by tab. By default, program mock commits data on the database for testing. By feeding extra argument, program will commit data without rolling them back. The program runs series of sanity checks to prevent incorrect uploads. All of cvproperty, type, and scrfeature must be mapped in the database before hand. 

# Copyright (c) 2016 Kitae Kim

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

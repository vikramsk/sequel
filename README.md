# sequel
A SQL database written from the ground up.

# Directory structure
The repository consists of the following:

 - **src** - Consists of the source files.
 - **data** - Consists of the TPC-H data files for running tests.
 - **include** - Consists of the dependencies(googletest, cpptoml).
 - **tests** - Consists of the google test scenarios.
 - **build** - This directory is created on running make and stores the build artifacts. 
 
# Build instructions
 - Define the file paths for the tpch folder and the catalog directory in `config.toml`.
 - Enter the root directory and run:
	 `make all`
 - This will create a build directory in the root folder along with the executables.
 - To run console driven tests for assignment 3 (relational operations), execute:
	 `./build/test.out`
 - To run console driven tests for assignment 2 (sorted file implementation), execute: 
	 `./build/test2.out`
 - To run console driven tests for assignment 1 (heap file implementation), execute: 
	 `./build/test1.out`
 - For running unit tests, execute: 
	 `./build/testsuite`
- For running the main program, execute: 
	 `./build/main`

# Contributors

 - Shikha Dharmendra Mehta (48519256)
 - Vikram Sreekumar (51088960)


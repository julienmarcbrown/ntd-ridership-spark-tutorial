# NTD Ridership Data Processing Pipeline

This project is a Spark pipeline to process the NTD ridership data available at Monthly Module Adjusted Data Release.

### Copyright Information

© 2023 Ganze Karte Software. All rights reserved.

### Owner Information

Owner: Julien Marc Brown
Contact: julien@ganzekarte.com

### Dependencies

To get the project up and running, ensure you have the following dependencies:

Scala: This project uses Scala version 2.13.12.
Apache Spark: This project is built on Apache Spark version 3.4.1.
Other Libraries:
spray-json: For JSON operations.
scalatest: For running tests.
spark-excel: For reading excel files using Spark.
postgresql: For database connections to PostgreSQL.
Setting up the Project
Clone the repository.
Ensure you have SBT (Scala Build Tool) installed.
Navigate to the project root and run:

```sbt compile```

### Project Structure

src/main/scala: Contains the main code for processing the data.
src/test/scala: Contains the test cases.

### Building and Running

To build the project:

```sbt package```
To run tests:

bash
sbt test

### Contributing

For contributing, please create a pull request, and one of the maintainers will look into it.


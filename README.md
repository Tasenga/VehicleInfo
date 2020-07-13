# README #

This README documents steps are necessary to get this application up and running.

### What is this repository for? ###

* This program converts customer data into an object with a specific structure using Apache Spark, and then saves it to the MongoDB database.
* Version 1.0.0.

### How do I get set up? ###

* Prerequisites:
    For Windows:
    1. install JAVA version 8 or higher;
    2. download Spark archive from https://spark.apache.org/downloads.html (Spark release 3.0.0 or higher pre-built for Apache Hadoop 2.7 or higher) and extract it;
    3. create SPARK_HOME and HADOOP_HOME to a directory which appeared on previous step;
    4. download relevant winutils.exe binary from https://github.com/steveloughran/winutils repository
    5. save winutils.exe binary to %HADOOP_HOME%\bin
    6. create c:\tmp\hive directory
    7. execute winutils.exe chmod -R 777 \tmp\hive

* Summary of set up:
    1. clone this repository into the local machine;
    2. create a virtual environment and activate it;
    3. make sure that the virtual environment uses python 3.8 or higher;
    4. install python packages from requirements.txt (pip install -r requirements.txt);
    5. run 'pre-commit install' to set up the git hook scripts;
    6. storage customer data into 'data_source' folder in root project directory;
    7. make sure that the MongoDB server has been run;
    8. prepare configuration file with .ini type (see section "Configuration") and save it in root project directory;
    9. run script from root project directory with using

           for Windows: 'python main.py -i name_your_configuration_file.ini'
           for Linux: 'python3 main.py -i name_your_configuration_file.ini'

* Configuration:

  The configuration file contains following parameters:

        source_folder: str (one word)
        db_name: str (one word)
        host: str (your MongoDB server host name)
        port: int (your MongoDB server port number)
        current_date: str (not necessary)
        current_timestamp: str (not necessary)

    For example, text in config.ini:

        [PARAMETERS]
        source_folder=data_source
        db_name=schwacke
        host=localhost
        port=27017

* Dependencies

    All dependencies are described in requirements.txt file.

* How to run tests

    Set Spark location to run pytest-spark how it is described on https://pypi.org/project/pytest-spark/.

    Run test from root project directory with using

           for Windows: 'python -m pytest'
           for Linux: 'python3 -m pytest'


### Author ###

* Anastasia Orlovskaya (nastassia.orlovskaya@gmail.com)
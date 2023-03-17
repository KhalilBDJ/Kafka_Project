#!/bin/bash
cd "$(dirname "$0")"
pwd
cd src/main/java
javac org/example/Console/RestConsoleApp.java
java org.example.Console.RestConsoleApp

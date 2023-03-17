@echo off
cd /D "%~dp0"
javac -d out src/main/java/org/example/Console/RestConsoleApp.java
java -cp out org.example.Console.RestConsoleApp
pause

@echo off
setlocal

if "%GRAALVM_HOME%"=="" (
    echo GRAALVM_HOME must point to a GraalVM JDK 22 installation 1>&2
    exit /b 2
)
if "%~2"=="" (
    echo usage: build-native.cmd CLASSES_DIR OUTPUT_DIR 1>&2
    exit /b 2
)

if not defined VCVARS64 set "VCVARS64=C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
if exist "%VCVARS64%" (
    call "%VCVARS64%" >nul
    if errorlevel 1 exit /b 3
)

set "JAVA_HOME=%GRAALVM_HOME%"
set "PATH=%GRAALVM_HOME%\bin;%PATH%"
set "CLASSES=%~f1"
if not exist "%~2" mkdir "%~2"
cd /d "%~f2"

set "FFM_FLAG="
if /i "%PROCESSOR_ARCHITECTURE%"=="AMD64" set "FFM_FLAG=-H:+ForeignAPISupport"

call native-image --shared -O3 --no-fallback %FFM_FLAG% -o libnodusdb -cp "%CLASSES%"
if errorlevel 1 exit /b 1

copy /Y libnodusdb.h nodusdb.h >nul
exit /b 0

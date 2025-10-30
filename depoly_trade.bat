@echo off
echo Creating Python 3.13 compatible Lambda Layer...

REM Clean up existing directory if it exists
if exist lambda_deployment (
  echo Removing existing lambda_deployment directory...
  rmdir /S /Q lambda_deployment
)

REM Create directories for deployment
mkdir lambda_deployment
cd lambda_deployment

REM Create Python directory structure for Lambda layer
mkdir python
cd python

REM Create a requirements.txt file with Python 3.13 compatible versions
echo Creating requirements.txt file...
(
echo pydantic==2.6.1
echo supabase==2.3.1
) > requirements.txt

REM Use pip to install from requirements file
echo Installing packages for Lambda...
python -m pip install --no-user --ignore-installed --target=. -r requirements.txt

REM Create zip file for Lambda layer:
echo Creating zip file for Lambda layer...
cd ..

REM Use explicit full path for zip file with correct extension
set ZIP_FILE=%cd%\lambda_layer_tarde_startegy.zip
echo Will create: %ZIP_FILE%

REM Ensure we have a proper .zip file (fixed extension)
powershell -Command "Compress-Archive -Path python -DestinationPath '%ZIP_FILE%' -Force"

echo Checking if zip file was created...
if exist "%ZIP_FILE%" (
  echo Lambda layer created successfully!
  echo Layer zip file location: %ZIP_FILE%
) else (
  echo ERROR: Failed to create zip file!
)

echo.
echo IMPORTANT: Keep using Python 3.13 runtime in AWS Lambda
echo Upload this zip file as a Lambda layer in the AWS console.

pause
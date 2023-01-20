S3 Bucket Metadata Management
===
## Paper Information
- Title:  `S3 Bucket Metadata Management`
- Author:  `Pedro Alejandro Pacheco Tripi`

## Install & Dependence
- python
- pip

## Environment Preparation
- Create the enviroment
  ```
  py -m venv <name_of_virtualenv>
  ```
- Activate the enviroment
  ```
  .\<name_of_virtualenv>\Scripts\activate
  ```
- Install libraries
  ```
  (<name_of_virtualenv>) py -m pip install -r requirements.txt
  ```
- Deactivate the enviroment (when the application finish)
  ```
  deactivate
  ```

## Use
- Activate the enviroment
  ```
  .\<name_of_virtualenv>\Scripts\activate
  ```
- Modify params into the code (param1, param2, param3, param4)
  ```
  param1 = ACCESS_KEY
  param2 = SECRET_KEY
  param3 = ENDPOINT_URL
  param4 = BUCKET_NAME
  ```
- Run the application
  ```
  py .\metadataS3.py
  ```
- Deactivate the enviroment
  ```
  deactivate
  ```


## Directory Hierarchy
```
|—— metadataS3.py
|—— requirements.txt
|—— README.md
|—— .gitignore
```
## Code Details
### Tested Platform
- Software
  ```
  Python: 3.11.1
  ```


## References
- [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)

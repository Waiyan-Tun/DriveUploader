## Create a Virtual Environment (optional but recommended)

python3 -m venv venv

source venv/bin/activate
pip install PyQt5




## install required packages
pip install pyqt5 mysql-connector-python google-api-python-client google-auth-httplib2 google-auth-oauthlib


## Check credentials.json
## Run the program
python DriveMySQLUploader.py

## EXE file creation
pip install pyinstaller
pyinstaller --onefile --noconsole DriveMySQLUploader.py

## EXE file will be in dist folder

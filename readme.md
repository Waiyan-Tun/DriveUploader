## Create a Virtual Environment (optional but recommended)

python -m venv myenv
myenv\Scripts\activate


## install required packages
pip install pyqt5 mysql-connector-python google-api-python-client google-auth-httplib2 google-auth-oauthlib


## Check credentials.json
## Run the program
python DriveMySQLUploader.py

## EXE file creation
pip install pyinstaller
pyinstaller --onefile --noconsole DriveMySQLUploader.py

## EXE file will be in dist folder
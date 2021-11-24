# Create virtualenv and build your python project with this
# directories for build are in setup.cfg
# assume that package name is hurma/ and output of build is in
# dist folder
virtualenv -p python3.8 venv
source venv/bin/activate
pip3 install -r requirements/requirements.txt
python3 -m pip install --upgrade build
python3 -m build

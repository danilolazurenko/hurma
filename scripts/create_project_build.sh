virtualenv -p python3 venv
source venv/bin/activate
pip3 install requirements/requirements.txt
python3 -m pip install --upgrade build
python3 -m build
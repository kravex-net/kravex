uv venv benchmark/.venv
source benchmark/.venv/bin/activate
uv pip install esrally

npm install -g elasticdump

python3 setup.py

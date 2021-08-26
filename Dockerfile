FROM python:3.8
RUN pip3 install https://github.com/waggle-sensor/pywaggle/archive/refs/tags/0.43.2.zip
COPY . .
ENTRYPOINT [ "python", "main.py" ]

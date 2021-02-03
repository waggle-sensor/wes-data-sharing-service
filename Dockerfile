FROM python:3.8-alpine
RUN pip3 install https://github.com/waggle-sensor/pywaggle/archive/v0.40.4.zip
COPY . .
ENTRYPOINT [ "python", "main.py" ]

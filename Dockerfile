FROM python:3.10-alpine
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT [ "python", "main.py" ]

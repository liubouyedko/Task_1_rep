FROM python:3.9
# ENV
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN ls -la /app

CMD ["python", "main.py"]

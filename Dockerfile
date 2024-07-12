FROM python:3.9
# ENV  
WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN ls -la /app

CMD ["python", "main.py"]
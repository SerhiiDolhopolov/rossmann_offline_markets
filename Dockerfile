from python:3.13-slim

WORKDIR /app  

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "-u", "app/main.py"]
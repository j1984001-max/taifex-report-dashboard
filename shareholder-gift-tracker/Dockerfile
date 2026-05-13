FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8765

WORKDIR /app

COPY server.py index.html app.js styles.css ./

EXPOSE 8765

CMD ["python", "server.py"]

FROM python:3-slim
WORKDIR /app
COPY clienteTCP.py /app
ENTRYPOINT ["python","clienteTCP.py"]
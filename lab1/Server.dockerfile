FROM python:3-slim
WORKDIR /app
COPY servidorTCP.py /app
CMD ["python","servidorTCP.py"]
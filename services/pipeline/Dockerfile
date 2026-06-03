FROM python:3.11-slim

WORKDIR /app

# System dependencies for OpenCV and confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    libgl1-mesa-glx \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download YOLOv8n weights so container starts faster
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')" || true

COPY . .

CMD ["python", "main.py"]

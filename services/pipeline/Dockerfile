FROM python:3.11-slim-bookworm

WORKDIR /app

# System dependencies for OpenCV and confluent-kafka
# Note: libgl1-mesa-glx was renamed to libgl1 in Debian Bookworm+
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    libgl1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download YOLOv8n weights so container starts faster
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')" || true

COPY . .

CMD ["python", "main.py"]

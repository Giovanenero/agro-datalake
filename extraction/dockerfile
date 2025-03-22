FROM debian:latest

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    python3 \
    python3-pip \
    python3-venv \
    xdotool \
    tesseract-ocr-por \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app/
RUN chmod -R 755 /app

RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install -r /app/requirements.txt

ENV PATH="/app/venv/bin:$PATH"

ENV DISPLAY=:0

CMD ["tail", "-f", "/dev/null"]

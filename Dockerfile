FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl unzip file aria2 \
    && rm -rf /var/lib/apt/lists/*

RUN ASSET_URL=$(curl -s https://api.github.com/repos/kaegi/alass/releases/latest \
      | grep "browser_download_url" | grep -i "linux" | cut -d '"' -f 4 | head -n 1) \
    && curl -L -o /tmp/alass_download "$ASSET_URL" \
    && mkdir -p /tmp/alass_bin \
    && (file /tmp/alass_download | grep -qi zip \
        && unzip -o /tmp/alass_download -d /tmp/alass_bin \
        || cp /tmp/alass_download /tmp/alass_bin/alass-cli) \
    && BIN_PATH=$(find /tmp/alass_bin -type f | head -n 1) \
    && mv "$BIN_PATH" /usr/local/bin/alass-cli \
    && chmod +x /usr/local/bin/alass-cli \
    && rm -rf /tmp/alass_download /tmp/alass_bin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /workspace

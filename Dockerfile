# Web tier for ivrit.ai Explore.
#
# Nothing large is baked in: episode audio is read from S3 and the search index
# lives in PostgreSQL, both configured through environment variables at run
# time. That keeps the image to the app itself plus ffmpeg.
FROM python:3.13-slim

WORKDIR /app

# ffmpeg backs the audio segment export route.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    EXPLORE_AUDIO_SOURCE=s3 \
    EXPLORE_INDEX_BACKEND=postgres

# TLS terminates at the platform's proxy, which forwards plain HTTP with
# X-Forwarded-Proto. uvicorn only honours that header from a trusted source and
# trusts just 127.0.0.1 by default, so without this every absolute URL the app
# generates comes out http:// and the browser blocks it as mixed content.
# Trust the private ranges the proxy lives in; override with FORWARDED_ALLOW_IPS.
ENV FORWARDED_ALLOW_IPS="10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,127.0.0.1,::1"

# $XHOST_HTTP_PORT is injected at container start, so expand it in a shell and
# exec so uvicorn receives stop signals directly.
CMD ["sh", "-c", "exec uvicorn wsgi:app --host 0.0.0.0 --port $XHOST_HTTP_PORT --proxy-headers --forwarded-allow-ips \"$FORWARDED_ALLOW_IPS\""]

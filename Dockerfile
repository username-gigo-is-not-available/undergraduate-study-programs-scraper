# Stage 1: Builder
FROM python:3.13.3-alpine as builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk update && \
    apk upgrade

WORKDIR /undergraduate-study-program-scraper

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.13.3-alpine

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apk update && \
    apk upgrade

RUN addgroup -S app_group && adduser -S app_user -G app_group

USER app_user
WORKDIR /undergraduate-study-program-scraper

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY ./src ./src

ENV PYTHONPATH=/undergraduate-study-program-scraper

CMD ["python", "src/main.py"]

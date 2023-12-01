FROM python:3.12-alpine as builder

# Install Rust and Cargo (needed to build fastparquet)
RUN apk update \
    && apk add g++ git openblas-dev rust cargo

# Add Cargo to PATH (if not automatically added)
ENV PATH="/root/.cargo/bin:${PATH}"

COPY . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install --pre aiohttp
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -r requirements.txt

# Final stage
FROM python:3.12-alpine

# Install runtime dependencies if any
RUN apk --no-cache add libstdc++

# Copy only the necessary files from the builder stage
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /app /app
WORKDIR /app

CMD ["python", "collect.py", "-f", "optionable.txt", "--destination", "s3://options-testing", "--now", "--email"]

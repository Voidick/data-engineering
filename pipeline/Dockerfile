FROM python:3.13.11-slim

# --- system deps (single layer) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg apt-transport-https wget \
 && rm -rf /var/lib/apt/lists/*

# --- uv ---
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

# --- python deps (cached) ---
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked

# --- Google Cloud CLI repo + install ---
RUN set -eux; \
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/google-cloud.gpg; \
  echo "deb [signed-by=/usr/share/keyrings/google-cloud.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list; \
  apt-get update; \
  apt-get install -y --no-install-recommends google-cloud-cli; \
  rm -rf /var/lib/apt/lists/*

# --- HashiCorp repo + install Terraform ---
# Option A (simple): install latest from repo
RUN set -eux; \
  wget -O- https://apt.releases.hashicorp.com/gpg \
    | gpg --dearmor -o /usr/share/keyrings/hashicorp.gpg; \
  . /etc/os-release; \
  echo "deb [signed-by=/usr/share/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com ${UBUNTU_CODENAME} main" \
    > /etc/apt/sources.list.d/hashicorp.list; \
  apt-get update; \
  apt-get install -y --no-install-recommends terraform; \
  rm -rf /var/lib/apt/lists/*

# --- app code ---
COPY ingest_data.py ./ingest_data.py

# Default: interactive shell (best for DE workflows: terraform/gcloud/python)
CMD ["bash"]

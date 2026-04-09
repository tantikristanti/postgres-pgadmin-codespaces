# Base Docker image that we will build on. 
# Start with slim Python 3.13 image for smaller size
FROM python:3.13.11-slim

# Copy uv binary from official uv image(multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory inside container
WORKDIR /app

# Add virtual environment in the working directory to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better caching) to the container current working directory (/app)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install all dependencies (pandas, sqlalchemy, psycopg2, click)
RUN uv sync --locked

# Copy ingestion script to the working directory
COPY data-ingestion.py . 

# Set entry point to run the ingestion script
ENTRYPOINT [ "python", "data-ingestion.py" ]

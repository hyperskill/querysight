FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including SSH client and PDF generation requirements
RUN apt-get update && apt-get install -y \
    build-essential \
    openssh-client \
    libpango-1.0-0 \
    libharfbuzz0b \
    libpangoft2-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create logs directory with proper permissions
RUN mkdir -p logs && chmod 777 logs

# Create cache directory for sampling wizard
RUN mkdir -p .cache && chmod 777 .cache

# Expose Streamlit port
EXPOSE 8501

# Command to run the application
CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0"]

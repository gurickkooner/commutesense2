FROM python:3.8-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app/

# Expose the port the app runs on
EXPOSE 5007

# Default command (will be overridden by docker-compose for data-cleaning service)
CMD ["bokeh", "serve", "--show", "commutesense.py", "--port", "5007", "--allow-websocket-origin=*"]
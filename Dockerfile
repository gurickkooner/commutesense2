# FROM python:3.8-slim

# # Set the working directory
# WORKDIR /app

# # Copy the requirements file
# COPY requirements.txt .

# # Install the dependencies
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy the bokeh_app directory contents to /app/bokeh_app in the container
# COPY bokeh_app /app/bokeh_app

# # Expose the port on which Bokeh will run
# EXPOSE 5007

# # Command to run Bokeh server
# CMD ["bokeh", "serve", "--show", "/app/bokeh_app", "--port", "5007", "--allow-websocket-origin=*"]




# FROM python:3.8-slim

# # Set the working directory
# WORKDIR /app

# # Copy the requirements file
# COPY requirements.txt .

# # Install the dependencies
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy the bokeh_app directory contents to /app/bokeh_app in the container
# COPY bokeh_app /app/bokeh_app

# # Expose the port on which Bokeh will run
# EXPOSE 5007

# # Command to run Bokeh server
# CMD ["bokeh", "serve", "--show", "/app/bokeh_app", "--port", "5007", "--allow-websocket-origin=*"]



FROM python:3.8-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bokeh_app directory contents to /app/bokeh_app in the container
COPY bokeh_app /app/bokeh_app

# Expose the port on which Bokeh will run
EXPOSE 5007

# Command to run Bokeh server
CMD ["bokeh", "serve", "--show", "/app/bokeh_app", "--port", "5007", "--address", "0.0.0.0", "--allow-websocket-origin=*"]

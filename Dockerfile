#############################
# Docker file for NDBView app
#############################

# Use an official Python runtime as a parent image
FROM python:2.7

MAINTAINER James Sample <james.sample@niva.no>

# Set the working directory to /ndbview
WORKDIR /ndbview

# Copy the current directory contents (i.e. the Flask app, setup.py etc.)
# into the container at /ndbview
ADD . /ndbview

# Install app
RUN python setup.py install

# Open port 5000 for external connections
EXPOSE 5000

# Define environment variables
ENV NAME ndbview_app
ENV APP_DIR /ndbview/ndbview
ENV FLASK_APP /ndbview/ndbview/ndbview.py

# Run app.py when the container launches
CMD ["flask",  "run",  "-h", "0.0.0.0", "-p",  "5000"]
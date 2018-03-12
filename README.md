# NDBView

Backend Flask "end points" for a RESA2 interim replacement app.

## 1. Quick start

  1. Create a clean Python 3.6 environment. If they are not already included by default, install `'pip'` and [`'setuptools'`](https://pypi.python.org/pypi/setuptools) (but these will be included in most cases, such as when creating environments with `'venv'` or `'virtualenv'`).
  
  2. Clone this repository, `cd` into the folder containing `setup.py`, and run
  
          python setup.py install
      
  3. In the same folder, set the environment variables for the app
  
          set FLASK_APP=ndbview
          set FLASK_DEBUG=1
          
     **Note:** On Linux the syntax will be slightly different. Probably
     
          $env:FLASK_APP="ndbview"
          $env:FLASK_DEBUG=1
         
  4. Start the app using 
  
          flask run

  5. Test the endpoints using e.g. [Postman](https://www.getpostman.com/)      

## 2. Background

RESA2 is becoming difficult to maintain. We have therefore agreed to create a new application as a temporary replacement that will:

 1. Provide similar functionality to the old RESA2, and
 
 2. Connect directly to the NIVADATABASE (alongside Aquamonitor), so we do not need to maintain two parallel systems.
 
Akos has already developed some code for the frontend. This repository provides the backend "end points" required to query the NIVADATABASE in a similar way to RESA2.

## 3. Installing

The new NIVA software stack takes a modular approach. The notes in this section provide a basic introduction to developing "containerised" solutions.

### 3.1. Installing for development

#### 3.1.1. Create an environment

Creating a new Python environment for each "app" project gives full control over package versions etc., which will avoid conflicts later. It also makes it easier to deploy the finished app using e.g. docker.

The standard way of creating environments in Python is using [`'venv'`](https://docs.python.org/3/library/venv.html), which is part of the standard library in Python 3, but needs installing separately for Python 2. For an overview of getting started with `venv`, see the [Flask documentation](http://flask.pocoo.org/docs/0.12/installation/#virtualenv). 

An alternative for users of Anaconda is simply to create a new `conda` environment and install the basic packages there, for example:

    conda create -n ndbview python=2.7 anaconda jupyter flask

Regardless of how you create your environment, for development it's also useful to have [Jupyter Lab](https://github.com/jupyterlab/jupyterlab) available. On `conda` this is achieved using:

    activate ndbview # Activate the new environment

then:

    conda install nodejs
    conda install -c conda-forge jupyterlab
    jupyter labextension install @jupyterlab/geojson-extension

#### 3.1.2. Install the application

Once you've created a clean environment, activate it (using e.g. `activate ndbview` on Anaconda), then clone the repository, `cd` into the folder containing `setup.py` and run:

    python setup.py install
    
#### 3.1.3. Run the application

The app can be run either directly from the Anaconda command line or via a Power Shell terminal in Jupyter Lab. The latter is more convenient for development. First, `cd` into the folder containing `setup.py`, then run:

    $env:FLASK_APP="ndbview"
    $env:FLASK_DEBUG=1
    flask run
    
You should then be able to visit [`http://127.0.0.1:5000/`](http://127.0.0.1:5000/) to see the app log-in screen.

**Note:** If you choose to run from the Anaconda command line, the syntax for the code above is slightly different:

    set FLASK_APP=ndbview
    set FLASK_DEBUG=1
    flask run

#### 3.1.4. Test the application

The Flask end points in `ndbview.py` are just "decorated" Python functions, designed to accept POSTed JSON containing the required function arguments and to return JSON to the frontend application. Once the Flask application is running, the easiest way to test the functions is by using "[Postman](https://www.getpostman.com/)" to send raw JSON to the end point and check that the returned JSON looks reasonable - see red highlighting on the image below.

<img src="images\postman_example.png" alt="Postman example" width="600"/>

#### 3.1.5. Deleting the development environment

On Anaconda, the development environment can be removed using:

    conda remove --name ndbview --all

### 3.2. Installing for deployment

"Containerised" deployment is achieved using [Docker](https://www.docker.com/). The instructions below describe installing and building locally, but ultimately we need to deploy to Google Cloud Platform. Grunde has some initial instructions for this [here](https://github.com/NIVANorge/flask_example#kubernetes-deployment).

#### 3.2.1. Install Docker locally

Download and install Docker Community Edition (Edge) from [here](https://store.docker.com/editions/community/docker-ce-desktop-windows). Use the default options (which may involve restarting your computer).

#### 3.2.2. Create a Docker image and install the app

The settings for a Docker image are specified in a plain text file called a `Dockerfile` (with no file extension). An example can be found [here](https://github.com/NIVANorge/ndbview/blob/master/Dockerfile). 

This file should be placed at the same level in the folder structure as the app's `setup.py`. You can then `cd` into this directory using Jupyter Lab's Power Shell and run:

    docker build -t ndbview .

(Where the `.` at the end means "*look for a Dockerfile in the current directory*").

#### 3.2.3. Launch the app from Docker

This section summarises some useful Docker commands (all of which can be run from the Jupyter Lab Power Shell).

##### List all available Docker images

       docker images
       
       
##### Remove/clean unwanted Docker images

       # Remove a specific image
       docker rmi ndbview 
       
       # Force-remove all "dangling" images
       docker rmi $(docker images --filter "dangling=true" -q --no-trunc) --force
       
      
##### Launch a container from an image (mapping port 5000 of the container to `localhost` port 5557)

       docker run --name=ndbview -d -p 5557:5000 ndbview
       
    
##### List containers
 
       docker ps    # Only currently active
       docker ps -a # Includes containers that have "exited", but are still named
       
       
##### Stop a container
 
       docker stop ndbview

##### Remove all "exited" containers
 
       docker rm $(docker ps -qa --no-trunc --filter "status=exited")
       
Having launched a container from an image and checked that the container is running successfully, you should be able to navigate to `localhost` on the specified port ([`http://localhost:5557/`](http://localhost:5557/) in the example above) and see the home page for your app.

**Note:** The home page for NDBView seems to work OK, but logging-in currently returns a **`500 - Internal Server Error`**. I haven't investiagted this in detail, but I guess it's due to the containerised version being unable to connect to the NIVADATABASE (?).

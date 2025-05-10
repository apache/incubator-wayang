# Project Outline

This project aims to develop a user-friendly, platform-agnostic software framework for Federated Learning (FL) setups using Apache WAYANG. It abstracts away the underlying execution platforms, allowing seamless deployment across heterogeneous environments. The software has been tested with a basic Stochastic Gradient Descent (SGD) setup to validate its functionality.

#  Class Overview

## Client Package

### `Client.java`  
**Location:** `src/main/java/org/client/`  
**Purpose:** Represents a federated learning client with identifying information.  
Stores the client’s unique URL and name, which are used for communication and identification in FL workflows.

### `FLClient.java`  
**Location:** `src/main/java/org/client/`  
**Purpose:** Implements a federated learning client using the Pekko actor model.  
Handles handshake, plan initialization, and computation requests from the server.  
Uses Apache WAYANG to build and execute logical dataflow plans on a specified platform (Java or Spark), enabling flexible backend execution.


### `FLClientApp.java`  
**Location:** `src/main/java/org/client/`  
**Purpose:** Entry point for launching a federated learning client as a Pekko actor.  
Initializes the actor system with configuration and starts the `FLClient` with its associated metadata (URL, ID, platform, and input data).

## Components package

### `FLJob.java`  
**Location:** `src/main/java/org/components/`  
**Purpose:** Orchestrates the federated learning job by initializing the server actor, managing client connections, and running iterative training.  
Encapsulates job configuration including the aggregation logic, stopping criteria, plan function, hyperparameters, and update rules.  
Handles key stages: handshake, distributing plans/hyperparameters, checking stopping criterion, running iterations, and updating global state.

### `FLSystem.java`  
**Location:** `src/main/java/org/components/`  
**Purpose:** Coordinates multiple federated learning jobs by managing clients, instantiating servers, and running registered jobs.  
Provides interfaces for job registration, execution, and retrieval of final results. Acts as the main system-level controller for managing FL lifecycles.


### `Aggregator.java`  
**Location:** `src/main/java/org/components/aggregator/`  
**Purpose:** Defines an aggregator responsible for combining the responses from multiple clients using a specified aggregation function.  
This class leverages an `AggregatorFunction` to apply the defined aggregation logic on client responses, which are passed along with server hyperparameters.

### `Criterion.java`  
**Location:** `src/main/java/org/components/criterion/`  
**Purpose:** Defines a criterion to determine whether the federated learning job should stop or continue.  
This class uses a user-defined function `shouldStop` that checks whether the job should halt based on the current values of the system, such as the progress or condition of the job.


### `Hyperparameters.java`  
**Location:** `src/main/java/org/components/hyperparameters/`  
**Purpose:** Manages the hyperparameters for both the server and client in a federated learning system.  
This class provides methods to update and retrieve hyperparameters for the server and client. It maintains separate maps for server-specific and client-specific hyperparameters.


### `AggregatorFunction.java`  
**Location:** `src/main/java/org/functions/`  
**Purpose:** Defines a functional interface for aggregating client responses in federated learning.  
The interface contains a single method, `apply`, that takes a list of client responses and a map of server hyperparameters, returning an aggregated result.  
This is used by the `Aggregator` class to apply custom aggregation logic.

### `PlanFunction.java`  
**Location:** `src/main/java/org/functions/`  
**Purpose:** Defines a functional interface for applying custom operations to a Wayang `JavaPlanBuilder`.  
The interface contains a single method, `apply`, which takes three arguments:  
- `Object a`: A custom object input.
- `JavaPlanBuilder b`: A `JavaPlanBuilder` instance for constructing the execution plan.
- `Map<String, Object> c`: A map containing additional parameters.

The method returns a `Operator`, which represents an operation in the Wayang execution plan.


## Messages Package
**Location:** `src/main/java/org/messages/`  
**Purpose:** Contains the various kinds of messages that are being used in the FL (Federated Learning) setup by the actor model.  
This package defines different message types exchanged between actors in the federated learning system, facilitating communication and synchronization between the server and clients during the training process.


## Server Package

### `Server.java`  
**Location:** `src/main/java/org/server/`  
**Purpose:** Represents a server in the Federated Learning (FL) system with a URL and name.

### `FLServer.java`

**Location:** `src/main/java/org/server/`  
**Purpose:** Represents a Federated Learning (FL) server in the system. This class handles client-server communication, hyperparameter synchronization, model updates, and iterative learning processes using the actor model.




# SGD Testing
We tested the SGD algorithm using our FL setup, with 3 clients and a server. The relevant code can be found in `src/test`.

## Test Execution

To start the testing process, you need to run the client tests. These simulate the behavior of the clients in the Federated Learning setup:

- `FLClientTest1.java`
- `FLClientTest2.java`  
- `FLClientTest3.java`

Once the clients start running, you can run the `FLIntegrationTest.java` file. This file starts the server and coordinates the SGD training process for 5 epochs. The server receives updates from the clients, aggregates them, and runs the SGD setup.

The current example using the `wayang-ml4all` package to run SGD. For a different algorithm, tha plan, hyperparameters, criterion and aggregator can be specified in the `FLIntegrationTest.java` class.
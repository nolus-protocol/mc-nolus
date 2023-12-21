# NOLUS PROTOCOL - Monte Carlo Simulation
***

## Description
  This project is simulation of the business logic behind the NOLUS protocol. The program is meant to simulate the work of the protocol in preset market conditions and teoretical contract counts. There are plenty of functional parameters that define the basic parameters needed for the protocol to work. You can find these in the config.json file. There is a multiprocessing solution that can be used for runing multiple simulations. There are 2 types of output data - sample and summary. The sample data type consists of multiple dataframes that contain data generated and used during the simulation on dayli basis. The summary data type consists of multiple dataframes that contains selected features from all the simulations ran. The simulation is to be used for testing and simulating scenarios with different initial conditions. 
  ***
## How to start a simulation
  **For convenience there is a sample ```config.json``` file containing a sample configuration. Also there are some required files as samples for how the initial conditions should be set and used.**
1. Check the config file and make sure that u have all the required initial conditions set. Keep in mind that the simulation works with time-series and some of the initial conditions are provided in the form of a .csv file that requires time-series data.
2. Select the client distribution for LP and LS clients
3. Start the simulation
4. Find the output. Keep in mind that the simulation outputs multiple files containing different types of information about the work of the platform
***
## Requirements 
  There is ```requirements.txt``` file that will help you set up the enviroment for the simulation

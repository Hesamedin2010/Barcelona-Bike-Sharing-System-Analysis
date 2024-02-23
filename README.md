Barcelona Bike-Sharing System Analysis

Welcome to the Barcelona Bike-Sharing System Analysis project repository! 
This project focuses on analyzing the bike-sharing system in Barcelona using historical data on station occupancy. 
By examining the usage patterns of bike stations, I aim to identify the most critical timeslots for each station, 
helping optimize bike availability and improve user experience.

Data Overview
The analysis utilizes two main datasets:
- register.csv: Contains historical information about the number of used and free slots for approximately 3000 stations from May 2008 to September 2008.
- station.csv: Provides descriptions of the stations, including station ID, latitude, longitude, and name.

Analysis Approach
For this analysis, I employ two different programming styles in PySpark:
- RDD-based Programming: Analyzing the data using Resilient Distributed Datasets (RDDs) for efficient processing.
- DataFrame-based Programming: Utilizing DataFrames for a more structured and convenient approach to data manipulation and analysis.

Repository Structure
├── RDD-based_Programming/
│   ├── Jupyter.ipynb - Spark RDD-based programming          # Jupyter notebook for RDD-based programming
│   ├── Python - Spark RDD-based programming                 # Executable Python script for RDD-based programming
│   └── PDF - Spark RDD-based programming                    # PDF version of the Jupyter notebook for RDD-based programming
├── DataFrame-based_Programming/
│   ├── Jupyter.ipynb - Spark DataFrame-based programming           # Jupyter notebook for DataFrame-based programming
│   ├── Python - Spark DataFrame-based programming                  # Executable Python script for DataFrame-based programming
│   └── PDF - Spark DataFrame-based programming                     # PDF version of the Jupyter notebook for DataFrame-based programming
├── data/
│   ├── register.csv           # Historical data on station occupancy
│   └── station.csv            # Description of bike stations
├── LICENSE                    # License information for the project
└── README.md                  # Detailed project overview, setup instructions, and usage guidelines

Getting Started
To replicate the analysis or explore the provided code and notebooks, follow these steps:
1. Clone this repository to your local machine.
2. Navigate to the respective directories (RDD-based_Programming/ or DataFrame-based_Programming/) containing the notebooks and Python scripts.
3. Run the provided Jupyter notebooks or Python scripts to analyze the bike-sharing data.
4. Ensure that you have PySpark installed in your environment to execute the code.

Contribution Guidelines
Contributions to this project are welcome! If you have any ideas for improvements, additional analyses, or bug fixes, feel free to open an issue or submit a pull request.

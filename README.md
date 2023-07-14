# introduction
Custom flows are defined here. Use Prefect cloud as orchestrate and use local machine as agent. 

# Get Started
1. go to https://app.prefect.cloud/ and login
2. run pip install prefect in your selected conda env/virtual env
3. run prefect cloud login if haven't
4. start agent by prefect agent start --pool default-agent-pool to start a worker
5. run flow.

# deploy flows in example_flow.py
python example_flow.py
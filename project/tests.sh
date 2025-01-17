#!/bin/bash

# Notice: If you want to use this test (and pipeline), you need API credentials from kaggle.
#
# Little guide:
# If you are logged in, go to the following link: https://www.kaggle.com/settings and
# create new API credentials under "API" using the "Create New Token" button.
# A JSON file with the API credentials is then downloaded.
#
# To set the API credentials, the username and key from the JSON file must be set in this script
# or an environment file in this folder with the name "kaggle_environment.env" must be created.
#
# Regardless of whether the environment file is used or the data is defined in this script,
# the following two lines must either be commented in or copied into the environment file and set accordingly
# export KAGGLE_USERNAME=<username>
# export KAGGLE_KEY=<key>

# Path to environment file with kaggle api credentials
kaggle_env_file="./project/kaggle_environment.env"

# Check if needed environment variables are already set (important for later usage with GitHub Actions)
if [ -z "$KAGGLE_USERNAME" ] || [ -z "$KAGGLE_KEY" ]; then
    # Check if environment file exists and then read it in
    if [ -f "$kaggle_env_file" ]; then
        source "$kaggle_env_file"
    else
        echo "Error: To use this test (and pipeline), the two environment variables KAGGLE_USERNAME and KAGGLE_KEY must be set."
        echo "For more information see the comment in this code file."
        exit 1
    fi
fi

# Install python packages if needed
echo "- - - - - Install python packages for test (and pipeline) - - - - -"
pip install pandas
pip install sqlalchemy
pip install kaggle
pip install pytest
echo "- - - - - Install of python packages completed - - - - -"

# After the environment for kaggle is set, the test can be executed
pytest ./project/tests.py

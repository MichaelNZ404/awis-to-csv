# How to use:
1. Initialize your virtual environment and run `pip install -r requirements.txt`
2. Copy the env template with `cp .env.example .env`
3. Create your IAM user (https://aws.amazon.com/awis/getting-started/)
3. Modify .env to add your IAM user access and secret access key. 
4. Run with `python main.py` 

## Notes:
The program expects the csv located at CSV_IN_NAME to have the same format as test.csv

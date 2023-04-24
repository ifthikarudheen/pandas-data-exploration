from flask import Flask

app = Flask(__name__)

import os
from flask import request
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
import psycopg2
@app.route('/upload', methods=['POST'])
# def upload_file():

#     columns_to_check =['ResponseId', 'MainBranch', 'Employment', 'RemoteWork','CodingActivities']
#     file = request.files['file']
#     file_extension = os.path.splitext(file.filename)[1]
#     if file_extension!='.csv':
#         return 'Failed' 

#     df = pd.read_csv(file)

#     if not all(col in df.columns for col in columns_to_check):
        
#         return 'Failed'
#     # file.save('/home/neo/Downloads/flask-pandas/'+ file.filename)

#     df['job_id'] = np.random.choice(np.arange(1, 100000), size=len(df), replace=False)
#     columns_to_check.append('job_id')

#     table_df = df.loc[:,columns_to_check]

#     connection_string = "postgresql://ifthikar:ifthik*123@localhost/pandas"
#     engine = create_engine(connection_string)

#     table_df.to_sql('Employee', engine)



#     return 'OK'


# if __name__ == '__main__':
#     app.run(debug=True)
def upload_file():
    try:
        # List of columns to check for in the uploaded CSV file
        columns_to_check = ['ResponseId', 'MainBranch', 'Employment', 'RemoteWork', 'CodingActivities']

        # Get the uploaded file and its extension
        file = request.files['file']
        file_extension = os.path.splitext(file.filename)[1]

        # Check if the file is in CSV format
        if file_extension != '.csv':
            raise Exception('File must be in CSV format')

        # Read the CSV file into a pandas dataframe
        df = pd.read_csv(file)

        # Check if all the required columns exist in the dataframe
        if not all(col in df.columns for col in columns_to_check):
            raise Exception('Required columns not found in CSV file')

        # Generate a random job ID for each row in the dataframe
        df['job_id'] = np.random.choice(np.arange(1, 100000), size=len(df), replace=False)
        columns_to_check.append('job_id')

        # Create a new dataframe with only the required columns
        table_df = df.loc[:, columns_to_check]

        # Establish a connection to the PostgreSQL database and write the dataframe to the 'Employee' table
        connection_string = "postgresql://ifthikar:ifthik*123@localhost/pandas"
        engine = create_engine(connection_string)
        table_df.to_sql('Employees', engine)

        # Return a success message
        return 'OK'

    except Exception as e:
        # Return an error message if an exception is raised
        return 'Failed: ' + str(e)


if __name__ == '__main__':
    app.run(debug=True)

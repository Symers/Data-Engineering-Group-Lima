from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import smtplib
import pandas as pd
import random as rand
import numpy as np

def dataCollection(**context):

    datafile = "/opt/airflow/dags/data_science_student_marks.csv"
    fileType = datafile.split('.')[-1].lower()
    
    #Check file type and use correct pandas function to read file
    if fileType == "csv":
        inputTable = pd.read_csv(datafile)
    #Only accepts excel files with one sheet
    elif fileType in ["xlsx", "xls"]:
        inputTable = pd.read_excel(datafile)

    #Store target table using first 10 rows
    targetTable = inputTable.head(10)

    #Randomly remove 20% of the data
    for column in inputTable:
        randomCells = np.random.rand(len(inputTable)) < 0.2
        inputTable.loc[randomCells, column] = np.nan

    #Randomly create outliers of 10% of the numeric data
    for column in inputTable.select_dtypes(include='number'):
        randomCells = np.random.rand(len(inputTable)) < 0.1
        inputTable.loc[randomCells, column] = inputTable.loc[randomCells, column] * 100
    
    #Store ruined table using first 10 rows
    ruinedTable = inputTable.head(10)

    #Push response dictionary to XCom
    context['ti'].xcom_push(key='inputTable', value=inputTable)
    context['ti'].xcom_push(key='targetTable', value=targetTable)
    context['ti'].xcom_push(key='ruinedTable', value=ruinedTable)


def dataTransformation(**context):
    #Pull in dataframe from XCom
    inputTable = context['ti'].xcom_pull(key='inputTable', task_ids='dataCollection')
    
    #Convert all column names to lower case and record changes
    loweredColumns = []
    originalColumns = inputTable.columns.tolist()
    inputTable.columns = [column.lower() for column in inputTable.columns]
    for x in range(len(originalColumns)):
        if originalColumns[x] != inputTable.columns[x]:
            loweredColumns.append(inputTable.columns[x])

    #Find numeric columns by checking top ten values for numeric data
    numericColumns = []
    yesAndNoColumns = []
    for column in inputTable:
        topTenValues = inputTable[column].dropna().head(10)
        numericCount = pd.to_numeric(topTenValues, errors='coerce').notna().sum()

        if numericCount > 0:
            numericColumns.append(column)

        #Check for yes/no columns
        yesNoCount = topTenValues.isin(["yes", "no", "Yes","No"]).sum()
        if yesNoCount > 0:
            yesAndNoColumns.append(column)

            
    #Get rid of exceptional values and record how many changed for each column
    columnsWithOutliers = []
    for column in numericColumns:
        inputTable[column] = pd.to_numeric(inputTable[column], errors="coerce")

        #Find the lower quantile of the column values
        lowerQuantile = inputTable[column].quantile(0.25)
        
        #Find the upper quantile of the values in each column
        upperQuantile = inputTable[column].quantile(0.75)
        
        #Find the inter quantile range of the values in each column
        interQuantileRange = upperQuantile - lowerQuantile

        #Define lower and upper bounds for outliers
        lowerBound = lowerQuantile - 1.5 * interQuantileRange
        upperBound = upperQuantile + 1.5 * interQuantileRange

        #Seperate outliers using the bounds
        outliers = (inputTable[column] < lowerBound) | (inputTable[column] > upperBound)

        #Record columns with outliers for transformation table
        if outliers.any():
            columnsWithOutliers.append(column)
        
        #Divide outliers by 100
        inputTable.loc[outliers, column] =  inputTable.loc[outliers, column] / 100

    #Convert yes/no columns to binary 1/0
    for column in yesAndNoColumns:
        inputTable[column] = inputTable[column].map({"yes": 1, "no": 0,"Yes": 1, "No": 0})

    #Push tables and columns to XCom
    context['ti'].xcom_push(key='columnsWithOutliers', value=columnsWithOutliers)
    context['ti'].xcom_push(key='loweredColumns', value=loweredColumns)
    context['ti'].xcom_push(key='yesAndNoColumns', value=yesAndNoColumns)
    context['ti'].xcom_push(key='numericColumns', value=numericColumns)
    context['ti'].xcom_push(key='transformedTable', value=inputTable)

def convertAndFillData(**context):
    #Pull from XCom
    inputTable = context['ti'].xcom_pull(key='transformedTable', task_ids='dataTransformation')
    numericColumns = context['ti'].xcom_pull(key='numericColumns', task_ids='dataTransformation')

    idColumnsReset = []
    #Fill in empty values
    for column in inputTable:
        #For id columns, reset the range to fix missing values 
        if "id" in column.lower():
            inputTable[column] = range(1, len(inputTable) + 1)
            idColumnsReset.append(column)
        #For numeric columns, find the median and fill NA values
        elif column in numericColumns:
            median = inputTable[column].median()
            inputTable[column] = inputTable[column].fillna(median)
        #For categorical columns, find the mode value and fill NA values
        else:
            mode = inputTable[column].mode(dropna=True)
            if not mode.empty:
                inputTable[column] = inputTable[column].fillna(mode[0])

    #Convert numeric columns with no decimals into integers
    numericToInteger = []
    for column in numericColumns:
        #Make sure all values are numeric
        numericColumn = pd.to_numeric(inputTable[column], errors="coerce")
        #Check if all values are integers 
        noDecimals = (numericColumn.dropna() % 1 == 0).all()
        if noDecimals == True:
            inputTable[column] = numericColumn.astype("Int64")
            numericToInteger.append(column)
        else:
            inputTable[column] = numericColumn

    context['ti'].xcom_push(key='idColumnsReset', value=idColumnsReset)
    context['ti'].xcom_push(key='numericToInteger', value=numericToInteger)
    context['ti'].xcom_push(key='filledTable', value=inputTable)

#Create Transformation Table
def createTransformationTable(**context):
    inputTable = context['ti'].xcom_pull(key='filledTable', task_ids='convertAndFillData')
    loweredColumns = context['ti'].xcom_pull(key='loweredColumns', task_ids='dataTransformation')
    idColumnsReset = context['ti'].xcom_pull(key='idColumnsReset', task_ids='convertAndFillData')
    numericColumns = context['ti'].xcom_pull(key='numericColumns', task_ids='dataTransformation')
    numericToInteger = context['ti'].xcom_pull(key='numericToInteger', task_ids='convertAndFillData')
    yesAndNoColumns = context['ti'].xcom_pull(key='yesAndNoColumns', task_ids='dataTransformation')
    columnsWithOutliers = context['ti'].xcom_pull(key='columnsWithOutliers', task_ids='dataTransformation')

    #Initialize empty df with the columns you want
    transformationTable = pd.DataFrame(columns=[
        "Column Name",
        "Lowercased",
        "Reset ID Column",
        "Fill in NA with Median",
        "Fill in NA with Mode",
        "Changed Type to Integer",
        "Swap Yes/No Values to 1/0",
        "Exceptional Values Cleaned",
    ])

    for column in inputTable:
        inputRow = {
            "Column Name" : column,
            "Lowercased" : "No",
            "Reset ID Column" : "No",
            "Fill in NA with Median" : "No",
            "Fill in NA with Mode" : "No",
            "Changed Type to Integer" : "No",
            "Swap Yes/No Values to 1/0" : "No",
            "Exceptional Values Cleaned" : "No",
        }

        if column in loweredColumns:
            inputRow["Lowercased"] = "Yes"

        if column in idColumnsReset:
            inputRow["Reset ID Column"] = "Yes"

        if column in numericColumns:
            inputRow["Fill in NA with Median"] = "Yes"
        else:
            inputRow["Fill in NA with Mode"] = "Yes"

        if column in numericToInteger:
            inputRow["Changed Type to Integer"] = "Yes"

        if column in yesAndNoColumns:
            inputRow["Swap Yes/No Values to 1/0"] = "Yes"

        if column in columnsWithOutliers:
            inputRow["Exceptional Values Cleaned"] = "Yes"

        transformationTable = pd.concat([transformationTable, pd.DataFrame([inputRow])], ignore_index=True)

    context["ti"].xcom_push(key="transformationTable", value=transformationTable)
    context["ti"].xcom_push(key="inputTable", value=inputTable)


def sendResponse(**context):
    #Pull cleaned responses and quality info from XCom
    inputTable = context['ti'].xcom_pull(key='inputTable', task_ids='createTransformationTable')
    targetTable = context['ti'].xcom_pull(key='targetTable', task_ids='dataCollection')
    ruinedTable = context['ti'].xcom_pull(key='ruinedTable', task_ids='dataCollection')
    transformationTable = context['ti'].xcom_pull(key='transformationTable', task_ids='createTransformationTable')

    #Convert to HTML for email
    inputTable = inputTable.head(10)
    htmlTable = inputTable.to_html(classes='data', border=1)
    htmlTargetTable = targetTable.to_html(classes='data', border=1)
    htmlRuinedTable = ruinedTable.to_html(classes='data', border=1)
    htmlTransformationTable = transformationTable.to_html(classes='data', border=1)

    # Compose message: For markers, fill in with actual email details
    sender_email = "aidan.symes@gmail.com"
    receiver_email = "aidan.symes@gmail.com" 
    password = "aqvk slhq lptr rety" 
    subject = f"Airflow Data Output: LIMA Pipeline Response Data "
    body = f"<h1>Example Response Data for: </h1><br><h1>Final Table</h1><br>{htmlTable}<br><br><h1>Target Table</h1>{htmlTargetTable}<br><h1>Ruined Table</h1><br>{htmlRuinedTable}<br><br><h1>Data Transformation Table</h1>{htmlTransformationTable}"

    # Send email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        message = f"Subject: {subject}\nContent-Type: text/html\n\n{body}"
        server.sendmail(sender_email, receiver_email, message)

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

#Define the DAG and tasks
with DAG('lima_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    data_collection_task = PythonOperator(
        task_id='dataCollection',
        python_callable=dataCollection
    )
    data_transformation_task = PythonOperator(
        task_id='dataTransformation',
        python_callable=dataTransformation
    )
    data_conversion_task = PythonOperator(
        task_id='convertAndFillData',
        python_callable=convertAndFillData
    )
    create_transformation_table_task = PythonOperator(
        task_id='createTransformationTable',
        python_callable=createTransformationTable
    )
    send_responses_task = PythonOperator(
        task_id='sendResponse',
        python_callable=sendResponse
    )

    data_collection_task >> data_transformation_task >> data_conversion_task >> create_transformation_table_task >> send_responses_task

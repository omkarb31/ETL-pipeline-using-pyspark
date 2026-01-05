# ETL pipeline using pyspark

A PySpark-based machine learning pipeline that trains a linear regression model to predict revenue based on business metrics, and automatically exports predictions to Snowflake for downstream analytics.

## Project Overview

This project implements an end-to-end ML workflow that:
- Loads business data from CSV files stored in Databricks FileStore
- Trains a Linear Regression model using PySpark MLlib
- Saves the trained model for future use
- Generates predictions on the entire dataset
- Exports results to Snowflake for business intelligence integration

## Features

- **Distributed Computing**: Leverages Apache Spark for scalable data processing
- **Model Persistence**: Saves trained models to DBFS for reusability
- **Cloud Integration**: Seamless data export to Snowflake data warehouse
- **Feature Engineering**: Automated vector assembly for ML-ready features

## Prerequisites

- Databricks environment with PySpark
- Apache Spark 3.x
- Python 3.7+
- Snowflake account with appropriate credentials
- Required Python libraries:
  - `pyspark`
  - `snowflake-connector-python` (Snowflake Spark connector)

## Data Schema

The input CSV file should contain the following columns:

| Column Name | Description |
|------------|-------------|
| `Quarter` | Business quarter identifier |
| `No_of_employee` | Number of employees |
| `Marketing_spend` | Marketing expenditure |
| `R&D_spend` | Research & Development expenditure |
| `Total_Customers` | Total customer count |
| `Revenue` | Target variable (actual revenue) |

## Installation

1. Upload your data file to Databricks FileStore at `/FileStore/tables/MOCK_DATA__1_.csv`

2. Ensure Snowflake connector is installed in your Databricks cluster:
   ```bash
   pip install snowflake-connector-python
   ```

3. Configure Snowflake credentials in the script (see Configuration section)

## Configuration

Update the Snowflake connection parameters in `main.py`:

```python
sfOptions = {
    "sfURL": "your_account.snowflakecomputing.com",
    "sfUser": "your_username",
    "sfPassword": "your_password",
    "sfDatabase": "your_database",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfSchema": "PUBLIC"
}
```

## Usage

Run the main script in your Databricks notebook or submit as a Spark job:

```python
python main.py
```

### Workflow Steps

1. **Initialize Spark Session**: Creates a Spark session named 'data'
2. **Load Data**: Reads CSV with schema inference and header detection
3. **Feature Engineering**: Combines independent variables into a feature vector
4. **Train/Test Split**: Splits data 75/25 for training and testing
5. **Model Training**: Trains Linear Regression model on training data
6. **Model Persistence**: Saves model to `dbfs:/FileStore/tables/saved_model/`
7. **Model Loading**: Demonstrates loading the saved model
8. **Prediction**: Generates predictions on the entire dataset
9. **Export**: Writes results to Snowflake `DATA` table in append mode

## Output

The pipeline creates a Snowflake table with the following schema:

| Column | Description |
|--------|-------------|
| `Quarter` | Business quarter |
| `No_of_employee` | Employee count |
| `Marketing_spend` | Marketing budget |
| `R&D_spend` | R&D budget |
| `Total_Customers` | Customer count |
| `Revenue` | Actual revenue |
| `prediction` | Predicted revenue from model |

## Model Details

- **Algorithm**: Linear Regression
- **Features**: Quarter, No_of_employee, Marketing_spend, R&D_spend, Total_Customers
- **Target**: Revenue
- **Framework**: PySpark MLlib

## File Structure

```
project/
│
├── main.py                          # Main pipeline script
├── README.md                        # This file
└── /FileStore/tables/
    ├── MOCK_DATA__1_.csv           # Input data
    └── saved_model/                # Saved ML model directory
```

## Model Persistence Location

- **Path**: `dbfs:/FileStore/tables/saved_model/`
- **Format**: PySpark LinearRegressionModel native format

## Snowflake Integration

The script uses the Snowflake Spark Connector to write data directly from Spark DataFrames to Snowflake tables. The connection uses:
- Append mode for incremental data loading
- Native Snowflake connector for optimized performance
- Direct DataFrame-to-table mapping

## Performance Considerations

- Use appropriate cluster sizing based on data volume
- Consider partitioning for large datasets
- Monitor Snowflake warehouse size for export operations
- Adjust train/test split ratio based on data size

## Security Notes

⚠️ **Important**: Never commit credentials to version control
- Use Databricks secrets management for production
- Implement proper access controls on Snowflake
- Rotate passwords regularly
- Use service accounts for automated workflows

## Troubleshooting

**Issue**: Model save fails
- **Solution**: Ensure DBFS path exists and has write permissions

**Issue**: Snowflake connection fails
- **Solution**: Verify credentials, network connectivity, and Snowflake connector installation

**Issue**: Schema inference errors
- **Solution**: Check CSV format, ensure proper headers, validate data types

## Future Enhancements

- [ ] Add model evaluation metrics (RMSE, R², MAE)
- [ ] Implement hyperparameter tuning
- [ ] Add data validation and quality checks
- [ ] Include logging and monitoring
- [ ] Create automated scheduling with Databricks Jobs
- [ ] Add model versioning and experiment tracking
- [ ] Implement incremental loading logic

## Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Submit a pull request

## License

This project is provided as-is for educational and commercial use.

## Support

For issues and questions:
- Check Databricks documentation: https://docs.databricks.com
- Snowflake connector docs: https://docs.snowflake.com/en/user-guide/spark-connector.html
- PySpark MLlib guide: https://spark.apache.org/docs/latest/ml-guide.html

## Authors

Data Science Team

## Acknowledgments

- Apache Spark community
- Databricks platform
- Snowflake data cloud

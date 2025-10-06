from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, IntegerType,
                             StringType, BooleanType,DoubleType)
from pyiceberg.partitioning import PartitionSpec
import os
import pandas
import polars
import pyarrow as pa

# start the catalog connection
catalog = load_catalog(name='local')

def create_namespace(namespace_name):
    """Creates a namespace in Iceberg"""
    namespace = catalog.create_namespace_if_not_exists(namespace_name)
    return namespace

# Sample data to be inserted into the iceberg table
product_schema = Schema(
    NestedField(field_id=1, name="ProductId", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="Name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="Category", field_type=StringType(), required=True),
    NestedField(field_id=4, name="Price", field_type=DoubleType(), required=True),
    NestedField(field_id=5, name="Stock", field_type=IntegerType(), required=True),
    NestedField(field_id=6, name="IsActive", field_type=BooleanType(), required=True)
)




if __name__ == "__main__":

    create_namespace("Sales")
    product_table = catalog.create_table_if_not_exists(
        identifier="Sales.Product",
        schema=product_schema
    )

    # Define data to feed our table
    product_data = pa.Table.from_pylist([
    {"ProductId": 1, "Name": "Bleach", "Category": "Manga", "Price": 9.99, "Stock": 102, "IsActive": True},
    {"ProductId": 2, "Name": "One Piece", "Category": "Manga", "Price": 11.49, "Stock": 250, "IsActive": True},
    {"ProductId": 3, "Name": "Attack on Titan", "Category": "Manga", "Price": 12.99, "Stock": 80, "IsActive": False},
    {"ProductId": 4, "Name": "Naruto", "Category": "Manga", "Price": 10.49, "Stock": 0, "IsActive": False},
    {"ProductId": 5, "Name": "Jujutsu Kaisen", "Category": "Manga", "Price": 13.75, "Stock": 190, "IsActive": True},
    {"ProductId": 6, "Name": "Chainsaw Man", "Category": "Manga", "Price": 8.95, "Stock": 330, "IsActive": True}
    ],
    schema=product_table.schema().as_arrow())

    # Insert data into table
    #product_table.append(df=product_data)

    # Validate
    print(product_table.scan().select('*').to_arrow().to_pandas())
from flytekit import task, workflow
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema
from flytekit import StructuredDataset
import pandas as pd

# PRIMITIVE TYPES
@task
def primitive_example(i: int, f: float, b: bool, s: str) -> str:
    print(f"Integer: {i}, Float: {f}, Boolean: {b}, String: {s}")
    return f"Primitives: {i}, {f}, {b}, {s}"

# COLLECTIONS
@task
def collection_example(lst: list[int], dct: dict[str, float]) -> str:
    print(f"List: {lst}, Dict: {dct}")
    return f"Collections: {lst}, {dct}"

# CUSTOM TYPES
# FlyteFile Example
@task
def file_example(file: FlyteFile) -> FlyteFile:
    print(f"File Path: {file.path}")
    return file

# FlyteSchema Example (For structured data like tables)
@task
def schema_example(schema: FlyteSchema) -> FlyteSchema:
    print(f"Schema columns: {schema.columns}")
    return schema

# StructuredDataset Example (e.g., Pandas DataFrame)
@task
def dataset_example(dataset: StructuredDataset) -> StructuredDataset:
    print(f"Dataset columns: {dataset.open(pd.DataFrame).columns}")
    return dataset

# WORKFLOW to integrate all examples
@workflow
def combined_example_workflow(
    i: int,
    f: float,
    b: bool,
    s: str,
    lst: list[int],
    dct: dict[str, float],
    file: FlyteFile,
    schema: FlyteSchema,
    dataset: StructuredDataset
) -> str:
    primitive_res = primitive_example(i=i, f=f, b=b, s=s)
    collection_res = collection_example(lst=lst, dct=dct)
    file_res = file_example(file=file)
    schema_res = schema_example(schema=schema)
    dataset_res = dataset_example(dataset=dataset)
    
    return f"Workflow Results: {primitive_res}, {collection_res}, {file_res}, {schema_res}, {dataset_res}"

# Example execution of the workflow
if __name__ == "__main__":
    # Input data
    file = FlyteFile(path="/path/to/file")
    schema = FlyteSchema(pd.DataFrame({'column1': [1, 2], 'column2': ['A', 'B']}))
    dataset = StructuredDataset(pd.DataFrame({'col1': [10, 20], 'col2': [30, 40]}))

    # Run the workflow
    print(combined_example_workflow(
        i=42, f=3.14, b=True, s="Flyte",
        lst=[1, 2, 3],
        dct={"a": 1.0, "b": 2.0},
        file=file,
        schema=schema,
        dataset=dataset
    ))
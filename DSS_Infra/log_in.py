# Connect to Dataiku

import dataikuapi

host = "https://dss-99cc8daa-303acfa6-dku.us-east-1.app.dataiku.io"
apiKey = "1DBdXnisTHYf3fsWNBPC7cXD530nWjhP"
client = dataikuapi.DSSClient(host, apiKey)

## Create Project

dss_projects = client.list_project_keys()
print(f"List of the projects: {dss_projects}")

## list_users

#dss_users = client.list_users()
#print(dss_users)

ProjectKey = dss_projects[0]
print(f"ProjectKey: {ProjectKey}")

project = client.get_project(ProjectKey)

builder = project.new_recipe("python")

## Upload Dataset

import time
x = time.localtime()
print(x.tm_sec)
name_dataset = f"mydataset{x.tm_sec}_{x.tm_wday}"
dataset = project.create_upload_dataset(name_dataset)

with open("./Data/ShippingData.csv", "rb") as f:
    dataset.uploaded_add_file(f, "./Data/ShippingData.csv")

# At this point, the dataset object has been initialized, but the format is still unknown, and the schema is empty, so the dataset is not yet usable We run autodetection
settings = dataset.autodetect_settings()
# settings is now an object containing the "suggested" new dataset settings, including the detected format andcompleted schema
settings.save()

builder = project.new_recipe("python")

# Set the input
builder.with_input(name_dataset)
# Create a new managed dataset for the output in the filesystem_managed connection
builder.with_new_output_dataset("grouped_dataset12", "filesystem_managed")

# Set the code - builder is a PythonRecipeCreator, and has a ``with_script`` method
builder.with_script("""
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
mydataset25_3 = dataiku.Dataset("mydataset25_3")
mydataset25_3_df = mydataset25_3.get_dataframe()


# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

test_df = mydataset25_3_df # For this sample code, simply copy input to output

# Write recipe outputs
test = dataiku.Dataset("test2")
test = test.head(10)
test.write_with_schema(test_df)
""")

recipe = builder.create()

print(recipe.get_status())

# recipe is now a ``DSSRecipe`` representing the new recipe, and we can now run it
#job = recipe.run()


# datasets = project.list_datasets()
# # Returns a list of DSSDatasetListItem
#
# for dataset in datasets:
#         # Quick access to main information in the dataset list item
#         print("Name: %s" % dataset.name)
#         print("Type: %s" % dataset.type)
#         print("Connection: %s" % dataset.connection)
#         print("Tags: %s" % dataset.tags) # Returns a list of strings
#
#         # You can also use the list item as a dict of all available dataset information
#         print("Raw: %s" % dataset)
#
#
# # builder = project.new_recipe("python")
# #
# # recipes = project.list_recipes()
# # # Returns a list of DSSRecipeListItem
# # for recipe in recipes:
# #         # Quick access to main information in the recipe list item
# #         print("Name: %s" % recipe.name)
# #         print("Type: %s" % recipe.type)
# #         print("Tags: %s" % recipe.tags) # Returns a list of strings
# #
# #         # You can also use the list item as a dict of all available recipe information
# #         print("Raw: %s" % recipe)
# #
# #
# # # import dataiku
# # # import pandas as pd, numpy as np
# # # from dataiku import pandasutils as pdu
# # # import os
# # #
# # # # Recipe inputs
# # # folder_path = dataiku.Folder("FuShmlsH").get_path()
# # #
# # # path_of_csv = os.path.join(folder_path, "dataset_01.csv")
# # #
# # # my_dataset = pd.read_csv(path_of_csv)pd
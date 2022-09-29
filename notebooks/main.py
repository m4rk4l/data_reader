# Databricks notebook source
# This command only needs to be executed outside Repos.
# MAGIC %pip install <location of wheel>


options = [("mergeSchema", "True")]
location = "abfss://pint-rdp-p-01@rdpp01dls2.dfs.core.windows.net/integration_layer/pre_pdm_zone/MD_NonRetail/marco/data/allocatedcollateral/"
df = read(spark, location, "delta", options)
display(df)
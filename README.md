## Overview
Node.js project that builds a DynamoDB database of Brazilian health facilities based on government provided CSV files (Dados Abertos).

The raw data used as input is available on the Brazilian Government's [open data site](http://dados.gov.br/dataset/cnes_ativo).

To use Redis instead of DynamoDB use [this project](https://github.com/rafaelrpinto/health-db).

## Bulding the database

This project was built using Node 8.1.0 and connects to a DynamoDB configured on the aws-config.js file.

Extract the .gz file located on the files folder. The correct structure should be:

- files/cnes.csv
- files/cnes.small.csv

To create a db with a small dataset run:

`npm run small`

To create the db with the full dataset run:

`npm run full`

## Ignored facilities

To improve performance and avoid maps full of markers we only map hospitals, clinics, mobile units, etc.

## Expected output:

TODO

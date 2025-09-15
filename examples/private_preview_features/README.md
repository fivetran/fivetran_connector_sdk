# Private Preview Features

The following features are in Private Preview. Please connect with our professional services to get more information about them and enable it for your connector.
- [Importing External Libraries and Drivers](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/private_preview_features/importing_external_drivers)
  - This feature enables you to install drivers in your connector environment by writing a `installation.sh` file in the `drivers` folder, in the same directory as your connector.py file. This script will be executed at the time of deploying your connector, before your connector.py is run to sync your data.
- [Sybase IQ](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/private_preview_features/sybase_iq)
  - This feature enables you to connect to Sybase IQ database using the `FreeTDS` driver and `PyODBC` by writing a `installation.sh` file in the `drivers` folder. This script will be executed at the time of deploying your connector, before your connector.py is run to sync your data.
- [Sybase ASE](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/private_preview_features/sybase_ase)
  - This feature enables you to connect to Sybase ASE database using the `FreeTDS` driver and `PyODBC` by writing a `installation.sh` file in the `drivers` folder. This script will be executed at the time of deploying your connector, before your connector.py is run to sync your data.
- [ibm_infomix_using_jaydebeapi](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples/private_preview_features/ibm_infomix_using_jaydebeapi)
  - This example shows how to connect and sync data from IBM Informix using Connector SDK. This example uses the `jaydebeapi` library with external JDBC Informix driver, using `installation.sh` file in the `drivers` folder, to connect to the Informix database and fetch data.

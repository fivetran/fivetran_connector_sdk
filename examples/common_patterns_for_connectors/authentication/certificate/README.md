# Certificate-Based API Authentication Example

This example demonstrates how to use certificates to authenticate with APIs using the `fivetran_connector_sdk` module. The example includes two methods for handling certificates: 
- using base64 encoded strings
- retrieving certificates from cloud storage.


### Using Base64 Encoded Strings

The [`connector.py`](./using_base64_encoded_certificate/connector.py) script demonstrates how to use base64 encoded strings for certificate-based authentication. The script includes functions to decode the certificate and key and use them to authenticate API requests.

To get the base64 encoded certificate string, you can use the following command. This reads a certificate file, encodes its content to base64, and prints the encoded string.

```bash
openssl base64 -in <CERTIFICATE_FILE>
```

The command will output the base64 encoded string, which you can use in your configuration.
> NOTE: If the certificate and private key are present in same file, pass the same encoded string for both `certificate` and `private_key` fields in the configuration.


### Retrieving Certificates from AWS Cloud Storage

To retrieve certificates from AWS cloud storage during runtime, you can modify the function to download the certificate and key from your cloud storage provider (e.g., AWS S3, Google Cloud Storage, Azure Blob Storage).

The [`connector.py`](retrieve_from_aws/connector.py) script demonstrates how to download certificates from a S3 bucket during runtime and use them to authenticate API requests.
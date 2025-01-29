# OAuth2 Refresh Token Hubspot Connector example

**Note:** There is an existing Fivetran connector for HubSpot that users can integrate directly on the dashboard [here](https://fivetran.com/docs/connectors/applications/hubspot#hubspot). This example is for reference purposes to integrate with a custom OAuth2 source that requires access token refresh.

## Prerequisites

1. **HubSpot Account:** 
   - If you don't have one, follow the steps [here](https://developers.hubspot.com/docs/guides/apps/public-apps/overview).

2. **Developer Account and HubSpot App:**
   - Create a developer account and create a HubSpot app with scopes and redirect URL: [ref](https://developers.hubspot.com/docs/reference/api/app-management/oauth)

3. **Fetch the Code:** 
   - Use this curl command to get the code appended to your redirect URL:

     ```bash
     curl --location '[https://app.hubspot.com/oauth/authorize?client_id=xxxxxx&scope=xxx&redirect_uri=xxxx](https://app.hubspot.com/oauth/authorize?client_id=xxxxxx&scope=xxx&redirect_uri=xxxx)'
     ```

4. **Fetch the Refresh Token:** 
   - Use the code obtained in step 3 and the following curl command to fetch the refresh token:

     ```bash
     curl --location --request POST '[https://api.hubapi.com/oauth/v1/token?grant_type=authorization_code&client_id=xxxxx&client_secret=xxxxxx&redirect_uri=xxxx&code=xxxx](https://api.hubapi.com/oauth/v1/token?grant_type=authorization_code&client_id=xxxxx&client_secret=xxxxxx&redirect_uri=xxxx&code=xxxx)' \
     --header 'Content-Type: application/x-www-form-urlencoded'
     ```

5. **HubSpot API Collection:** 
   - Access the HubSpot API collection [here](https://developers.hubspot.com/docs/reference/api/crm/objects).

## Debug

1. **Replace Credentials:** Once you have the refresh token, client secret, and ID, replace them in the `configuration.json` file.
2. **Run the Main Function:** Run the main function to trigger the debug command and start syncing your code to your local machine.

## Deploy

1. **Fivetran API Key:** 
   - Get your base64 API key from the Fivetran dashboard: [https://fivetran.com/dashboard/user/api-config](https://fivetran.com/dashboard/user/api-config)

2. **Fivetran Destination:** 
   - Create a required destination from the Fivetran dashboard: [https://fivetran.com/dashboard/destinations](https://fivetran.com/dashboard/destinations)

3. **Deploy the Connector:** 
   - Use the following command in the folder containing the `connector.py` file to deploy:

     ```bash
     python connector.py --api-key <FIVETRAN-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME> --configuration configuration.json
     ```

4. **Monitor Sync Status:** 
   - Once deployed, follow the link in the terminal or search in the dashboard with the connection name to view the sync status and logs.

**Note:** This example only supports cases where the refresh token does not have a TTL, and only the access token is refreshed with the refresh token. If you occasionally need to update the refresh token, you can do it via the dashboard and in the connection setup. We will update this example with a similar approach once we support refreshing passed credentials via the connector code.
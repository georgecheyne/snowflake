# Running DBT locally

We use an inspection proxy for accessing external sources. This intercepts the SSL connection with Snowflake and presents its own certificates to us, the client.

Unfortunately, because DBT is using OpenSSL it doesn't use the Windows certificate store So we need perform the following steps to execute project commands from our machines.

1) Open a terminal and execute the following
```
openssl s_client -connect <ACCOUNT NAME>.snowflakecomputing.com:443 -showcerts
```
2) Copy the contents of the output (example below) ensuring you include all the certificates in the output down to the final END CERTIFICATE. Paste to a text file and save it in the pnl_warehouse folder as snowflake.pem
```
Certificate chain
0 s:C=US, ST=Montana, L=Bozeman, O=Snowflake Inc., CN=*.eu-west-1.snowflakecomputing.com
 .
 .
-----BEGIN CERTIFICATE----- 
 MIIHwDCCBqigAwI...
 -----END CERTIFICATE-----
 .
 .
 .
 .
 -----BEGIN CERTIFICATE----- 
 MIIHwDCCBqigAwI...
 -----END CERTIFICATE-----
```
3) Create a virtual python environment. 
```
python -m venv venv
```
4) Activate the environment (if using cmd.exe)
```
venv\Scripts\activate.bat
```
5) Install DBT
```
python -m pip install dbt-core dbt-snowflake
```
6) Edit the profiles.yml file 
```yaml
pnl_warehouse:
  target: dev
  outputs:
    dev:
      type: snowflake
      role: <ROLE>
      warehouse: <WAREHOUSE>
      database: <DATABASE>
      schema: <SCHEMA>
      account: 'BLUECREST-PROD'
      user: '<USER.NAME>@bluecrestcapital.com'
      authenticator: externalbrowser
      threads: 1 
```
7) Execute DBT

    We need to set an environment variable to point the snowflake pem file created earlier, before running. There's a couple of ways to do this but I settled on using Python.

    Create a new python file (eg dbt-command.py):
```python
import os
import subprocess

os.environ['REQUESTS_CA_BUNDLE'] = 'snowflake.pem'
subprocess.run(["dbt", "compile"])
```

```cmd
python dbt-command.py
```




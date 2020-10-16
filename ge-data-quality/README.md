# ge-data-quality pipeline 

# ge-data-quality

## Description
Great expectations is a library for python to review quality of data through suites of expectations that should comply data, and generate a documentation of the review (actually configured to save in s3 bucket yapo-s3-dev-data)
In folder app you would find principal file:

great_expectations.yaml is file of config of library, contain data source, notifications config, data storage of generated documentation.

In folder app/pipes you will find folder exp_suites (expectations_suites) that contain one pipeline of verifications over the realized query (or over a table) and custom_expectations for define news expectations to review in verification process



## Pipeline Implementation Details

|   Field           | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| Input Source      | Data warehouse                                                             |
| Output Source     | S3 Documentation in yapo-s3-dev-data bucket                                |
| Schedule          | 12:00                                                                      |
| Rundeck Access    | data jobs - DATA-QUALITY/Data - Great Expectation verifications            |


### Build
```
make docker-build
```

### Run micro services
```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/ge-data-quality:[TAG]
```

### Run micro services with parameters

```
docker run -v /local-path/secrets/pulse:/app/pulse-secret \
           -v /local-path/secrets/db-secret:/app/db-secret \
           -e APP_PULSE_SECRET=/app/pulse-secret \
           -e APP_DB_SECRET=/app/db-secret \
           containers.mpi-internal.com/yapo/ge-data-quality:[TAG] \
           -date_from=YYYY-MM-DD \
           -date_to=YYYY-MM-DD
```

### Adding Rundeck token to Travis

If we need to import a job into Rundeck, we can use the Rundeck API
sending an HTTTP POST request with the access token of an account.
To add said token to Travis (allowing travis to send the request),
first, we enter the user profile:
```
<rundeck domain>:4440/user/profile
```
And copy or create a new user token.

Then enter the project settings page in Travis
```
htttp://<travis server>/<registry>/<project>/settings
```
And add the environment variable RUNDECK_TOKEN, with value equal
to the copied token

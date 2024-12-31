
These files contain code to scrape from the FFIEC's call report filing website. They are formatted to be run on AWS batch (though the Aurora integration may be a little bit out of date at this point), and each one runs independently and should be relatively fault tolerant. The keypoint
is the BPORretreiver, which will create the table that lets the other scrapers know to pull more data.

The FFIEC releases this data source every quarter, and it can be found [here](https://cdr.ffiec.gov/public/pws/downloadbulkdata.aspx).

Docker containers need 3 environment variables:

1. aurora_endpoint (where's your database)
2. secret_name (where did you store the password to acess this database)
3. region_name (where is this database)

It would be trivial to configure this to not do AWS, just write your own get_secret function. This one was directly ported from the AWS examples docs in 2022. 
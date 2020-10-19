# datacity-ckan-dgp

## Install

Create .env file

```
bin/generate_key_pair.sh > .env
```

Generate Google OAuth credentials - https://console.developers.google.com/apis/credentials

```
echo GOOGLE_KEY=XXXX >> .env
echo GOOGLE_SECRET=YYYY >> .env
```

Start a datacity CKAN instance for local development, see https://github.com/hasadna/ckanext-datacity/blob/master/README.md

Get an admin CKAN API key from that instance and set in .env

```
echo CKAN_INSTANCE_LOCAL_DEVELOPMENT_API_KEY=XXXXX >> .env
echo CKAN_INSTANCE_LOCAL_DEVELOPMENT_URL=http://172.17.0.1:5000 >> .env
```

## Use

Start the datacity CKAN instance for local development, see https://github.com/hasadna/ckanext-datacity/blob/master/README.md

Start the dgp server

```
docker-compose up
```

http://localhost:15000

Login with your Google account

Set admin status:

```
docker-compose exec server bash -c "source env.sh && python mk_admin.py"
```

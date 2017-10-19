# athenaq

command line tool to execute athena queries 


### install:

```shell
go get -u github.com/advincze/athenaq
```


### use:

```shell
athenaq -h
Usage of athenaq:
  -athena.s3.path string
    	athena result bucket (default "s3://aws-athena-query-results-{{.Account}}-{{.Region}}/Unsaved/{{.Now.Format \"2006\"}}/{{.Now.Format \"01\"}}/{{.Now.Format \"02\"}}")
  -region string
    	aws region (default "eu-west-1")
  -timeout duration
    	athena query timeout (default 30m0s)
  -val value
    	(repeated) values separated by '='. e.g. key=val
```

### examples:

```shell
athenaq <<< "show databases"
```

```shell
athenaq <<< "show tables"
```


if you have an existing table in athena
```shell
athenaq <<< "select * from users limit 10"
```




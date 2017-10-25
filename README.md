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
  -dry
    	dry run
  -f string
    	input file (""== STDIN)
  -out string
    	output path ("-" == no output| "" == STDOUT | file://... | s3://...)
  -region string
    	aws region (default "eu-central-1")
  -temp.path string
    	athena result bucket (default "s3://aws-athena-query-results-{{ Account }}-{{ .Region }}/Unsaved/{{ Now.Format \"2006\"}}/{{ Now.Format \"01\" }}/{{ Now.Format \"02\"}}")
  -timeout duration
    	athena query timeout (default 1h0m0s)
```

the query is loaded from `STDIN`

for templating you can use the default go [text/template](https://golang.org/pkg/text/template/) engine

all environment variables are passed to the template.

additionaly all top level functions from [`strings`](https://golang.org/pkg/strings) are registered 

e.g. [`strings.Split`](https://golang.org/pkg/strings/#Split) is registered in the [`template.FuncMap`](https://golang.org/pkg/text/template/#Template.Funcs)

if you need any more functions, just let me know




either string:
```shell
athenaq <<< "show databases"
```
or:
```shell
echo "show databases" | athenaq
```


or a file
```shell
athenaq < myquery.sql
```
or:
http://porkmail.org/era/unix/award.html:
```shell
cat myquery.sql | athenaq 
```


### examples:

```shell
athenaq <<< "show databases"
```

```shell
athenaq <<< "show tables"
```

if you have an existing table in athena:
```shell
athenaq <<< "select * from users limit 10"
```

same thing with variables:
```shell
TABLE=users LIM=10 athenaq <<< "select * from {{ .TABLE }} limit {{ .LIM }}"
```




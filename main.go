package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/advincze/s3path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
)

type valueFlags map[string]string

func (i *valueFlags) String() string {
	keys := make([]string, 0, len(*i))
	for k := range *i {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s=%s\n", k, (*i)[k])
	}
	if l := buf.Len(); l > 0 {
		buf.Truncate(l - 1)
	}
	return buf.String()
}

func (i *valueFlags) Set(value string) error {
	ss := strings.SplitN(value, "=", 2)
	if len(ss) != 2 {
		return fmt.Errorf("wrong value %q", value)
	}
	(*i)[ss[0]] = ss[1]
	return nil
}

func main() {
	var (
		timeout              = flag.Duration("timeout", time.Minute*30, "athena query timeout")
		athenaS3PathTemplate = flag.String("temp.path", `s3://aws-athena-query-results-{{.Account}}-{{.Region}}/Unsaved/{{.Now.Format "2006"}}/{{.Now.Format "01"}}/{{.Now.Format "02"}}`, "athena result bucket")
		awsRegion            = flag.String("region", "eu-west-1", "aws region")
		values               = valueFlags(map[string]string{})
	)
	flag.Var(&values, "val", "(repeated) values separated by '='. e.g. key=val")
	flag.Parse()

	awsSession := session.New(aws.NewConfig().WithRegion(*awsRegion))
	awsCli := &awsCli{
		sts:    sts.New(awsSession),
		s3:     s3.New(awsSession),
		athena: athena.New(awsSession),
	}

	accountID, err := awsCli.AccountID()
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not retrieve account ID: %v", err)
		os.Exit(1)
	}

	athenaS3Path := execTemplate(*athenaS3PathTemplate, struct {
		Account, Region string
		Now             time.Time
	}{accountID, *awsRegion, time.Now()})

	err = awsCli.CreateBucketIfNotExists(athenaS3Path, *awsRegion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not create athena temp bucket: %v", err)
		os.Exit(1)
	}

	sql, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read query from STDIN: %v", err)
		os.Exit(1)
	}

	query := execTemplate(string(sql), values)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	queryExecution, err := awsCli.executeQuery(ctx, query, athenaS3Path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not execute athena query: %v", err)
		os.Exit(1)
	}

	data, err := awsCli.getS3Contents(ctx, *queryExecution.ResultConfiguration.OutputLocation)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not get s3 contents: %v", err)
		os.Exit(1)
	}

	if len(data) > 0 {
		fmt.Print(string(data))
	}
}

func execTemplate(tmpl string, val interface{}) string {
	var buf bytes.Buffer
	template.Must(template.New("").Parse(tmpl)).Execute(&buf, val)
	return buf.String()
}

type awsCli struct {
	sts    *sts.STS
	s3     *s3.S3
	athena *athena.Athena
}

func (awsCli *awsCli) CreateBucketIfNotExists(path, region string) error {
	s3url, err := s3path.Parse(path)
	if err != nil {
		return err
	}

	_, err = awsCli.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: &s3url.Bucket,
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: &region,
		},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case s3.ErrCodeBucketAlreadyExists, s3.ErrCodeBucketAlreadyOwnedByYou:
				return nil
			}
		}
		return err
	}
	return nil
}

func (awsCli *awsCli) AccountID() (string, error) {
	getCallerIdentityOut, err := awsCli.sts.GetCallerIdentity(nil)
	if err != nil {
		return "", err
	}
	return *getCallerIdentityOut.Account, nil
}

func (awsCli *awsCli) executeQuery(ctx context.Context, sql, outPath string) (*athena.QueryExecution, error) {
	startQueryExecutionOut, err := awsCli.athena.StartQueryExecutionWithContext(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(outPath),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("could not start query execution: %v", err)
	}

	t := time.NewTicker(time.Millisecond * 500)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("query got cancelled")
		case <-t.C:
			getQueryExecutionOut, err := awsCli.athena.GetQueryExecutionWithContext(ctx, &athena.GetQueryExecutionInput{
				QueryExecutionId: startQueryExecutionOut.QueryExecutionId,
			})
			if err != nil {
				return nil, fmt.Errorf("could not get query status: %v", err)
			}
			switch *getQueryExecutionOut.QueryExecution.Status.State {
			case "FAILED", "CANCELLED":
				return getQueryExecutionOut.QueryExecution, fmt.Errorf("athena query could not finish: %v", *getQueryExecutionOut.QueryExecution.Status.StateChangeReason)
			case "SUCCEEDED":
				return getQueryExecutionOut.QueryExecution, nil
			default:
				continue
			}
		}
	}
}

func (awsCli *awsCli) getS3Contents(ctx context.Context, path string) ([]byte, error) {
	s3Path, err := s3path.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("error parsing s3 URL: %v", err)
	}

	getObjOut, err := awsCli.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &s3Path.Bucket,
		Key:    &s3Path.Key,
	})
	if err != nil {
		return nil, fmt.Errorf("could not get result from  %q: %v", s3Path, err)
	}

	defer getObjOut.Body.Close()

	data, err := ioutil.ReadAll(getObjOut.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read result form s3: %v", err)
	}

	return data, nil
}

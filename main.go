package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/advincze/s3path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
)

func main() {
	var (
		timeout              = flag.Duration("timeout", time.Minute*30, "athena query timeout")
		athenaS3PathTemplate = flag.String("athena.s3.path", `s3://aws-athena-query-results-{{.Account}}-{{.Region}}/Unsaved/{{.Now.Format "2006"}}/{{.Now.Format "01"}}/{{.Now.Format "02"}}`, "athena result bucket")
		awsRegion            = flag.String("region", "eu-west-1", "aws region")
	)
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

	sql, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read query from STDIN: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	queryExecution, err := awsCli.executeQuery(ctx, string(sql), athenaS3Path)
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

func splitS3Path(path string) (bucket, key string, err error) {
	outURL, err := url.Parse(path)
	if err != nil {
		return "", "", fmt.Errorf("could not parse s3 URL %q: %v", path, err)
	}
	if outURL.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid scheme in s3 URL: %q ", outURL.Scheme)
	}
	return outURL.Host, strings.TrimLeft(outURL.Path, "/"), nil
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

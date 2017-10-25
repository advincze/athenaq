package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/pkg/errors"

	"github.com/advincze/s3path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
)

func main() {
	var (
		timeout              = flag.Duration("timeout", time.Minute*60, "athena query timeout")
		athenaS3PathTemplate = flag.String("temp.path", `s3://aws-athena-query-results-{{ Account }}-{{ .Region }}/Unsaved/{{ Now.Format "2006"}}/{{ Now.Format "01" }}/{{ Now.Format "02"}}`, "athena result bucket")
		awsRegion            = flag.String("region", "eu-central-1", "aws region")
		output               = flag.String("out", "", `output path ("-" == no output| "" == STDOUT | file://... | s3://...)`)
		inputFile            = flag.String("f", "", `input file (""== STDIN)`)
		dry                  = flag.Bool("dry", false, "dry run")
	)
	flag.Parse()

	awsCli, err := newAWS(*awsRegion, *athenaS3PathTemplate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not initialize aws client: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var input io.Reader
	switch *inputFile {
	case "":
		input = os.Stdin
	default:
		f, err := os.Open(*inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could open input file: %v", err)
			os.Exit(1)
		}
		defer f.Close()
		input = f
	}

	queries, err := readQueries(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read queries: %v", err)
		os.Exit(1)
	}

	var out io.Writer
	switch *output {
	case "-":
		out = nil
	case "":
		out = os.Stdout
	default:
		var buf bytes.Buffer
		defer func() {
			err := awsCli.writeOut(bytes.NewReader(buf.Bytes()), *output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could write result: %v", err)
				os.Exit(1)
			}
		}()
		out = &buf
	}

	for _, query := range queries {
		if *dry {
			fmt.Println("execute query:", query)
			continue
		}
		err = awsCli.execQuery(ctx, query, out)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not execute athena query: %v", err)
			os.Exit(1)
		}
	}
}

func readQueries(r io.Reader) ([]string, error) {
	in, err := ioutil.ReadAll(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not read input: %v", err)
		os.Exit(1)
	}
	var queries []string
	for _, s := range strings.Split(string(in), ";") {
		if strim := strings.TrimSpace(s); strim != "" {
			query, err := execTemplate(strim, nil, nil)
			if err != nil {
				return nil, errors.Wrap(err, "could not render query")
			}

			queries = append(queries, query)
		}
	}

	return queries, nil
}

type awsCli struct {
	sts        *sts.STS
	s3         *s3.S3
	athena     *athena.Athena
	athenaPath string
}

func newAWS(region, athenaPathTemplate string) (*awsCli, error) {
	awsSession := session.New(aws.NewConfig().WithRegion(region))
	awsCli := &awsCli{
		sts:    sts.New(awsSession),
		s3:     s3.New(awsSession),
		athena: athena.New(awsSession),
	}

	athenaS3Path, err := execTemplate(athenaPathTemplate, map[string]interface{}{
		"Account": awsCli.AccountID,
		"Now":     time.Now,
	}, struct{ Region string }{region})
	if err != nil {
		return nil, errors.Wrap(err, "could not render athena s3 path")
	}

	err = awsCli.CreateBucketIfNotExists(athenaS3Path, region)
	if err != nil {
		return nil, errors.Wrap(err, "could not create athena temp bucket")
	}

	awsCli.athenaPath = athenaS3Path

	return awsCli, nil
}

func (awsCli *awsCli) writeOut(r io.ReadSeeker, outPath string) error {
	p, _ := url.Parse(outPath)
	switch p.Scheme {
	case "", "file":
		fileName := path.Join(p.Host, p.Path)
		data, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		return ioutil.WriteFile(fileName, data, 0644)
	case "s3":
		bucket := p.Host
		key := strings.TrimLeft(p.Path, "/")
		if bucket == "" || key == "" {
			return fmt.Errorf("s3 bucket or key empty in %q", outPath)
		}
		_, err := awsCli.s3.PutObject(&s3.PutObjectInput{
			Body:   r,
			Bucket: &bucket,
			Key:    &key,
		})
		if err != nil {
			return errors.Wrap(err, "could not upload result to s3")
		}
	default:
		return fmt.Errorf("UNKNOWN: schema %q", outPath)
	}
	return nil
}

func (awsCli *awsCli) execQuery(ctx context.Context, query string, w io.Writer) error {
	queryExecution, err := awsCli.executeQuery(ctx, query)
	if err != nil {
		return errors.Wrap(err, "could not execute athena query")
	}

	if w != nil {
		data, err := awsCli.getS3Contents(ctx, *queryExecution.ResultConfiguration.OutputLocation)
		if err != nil {
			return errors.Wrap(err, "could not get s3 contents")
		}
		_, err = io.Copy(w, bytes.NewReader(data))
		return err
	}

	return nil
}

func execTemplate(tmpl string, funcs map[string]interface{}, values interface{}) (string, error) {
	var buf bytes.Buffer
	if values == nil {
		m := map[string]string{}
		for _, e := range os.Environ() {
			pair := strings.SplitN(e, "=", 2)
			m[pair[0]] = pair[1]
		}
		values = m
	}
	f := template.FuncMap{}
	for k, v := range funcs {
		f[k] = v
	}
	f["Split"] = strings.Split

	err := template.Must(template.New("").
		Funcs(f).
		Parse(tmpl)).Execute(&buf, values)

	return buf.String(), err
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
		return "", errors.Wrap(err, "could not get caller identity")
	}
	return *getCallerIdentityOut.Account, nil
}

func (awsCli *awsCli) executeQuery(ctx context.Context, sql string) (*athena.QueryExecution, error) {
	startQueryExecutionOut, err := awsCli.athena.StartQueryExecutionWithContext(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(awsCli.athenaPath),
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

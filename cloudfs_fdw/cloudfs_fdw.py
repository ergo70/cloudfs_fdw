import csv
import smart_open
import ijson
import pandas
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG


class cloudfs_fdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing CSV, JSON, EXCEL and ODF files on cloud filesystems.

    Valid options:
        - source : the data source (S3, HTTP/S, file, FTP, SCP, HDFS)
          Default: "S3"
        - format : the file format (CSV, JSON, EXCEL, ODF, plain or compressed)
          Default : CSV
        - host : hostname
        - port : port
        - region : the AWS region (S3 only)
        - aws_access_key : AWS access keys (S3 only)
        - aws_secret_key : AWS secret keys (S3 only)
        - bucket : bucket (S3 only)
        - filepath : full path to the csv file, which must be readable
        - delimiter : the delimiter used between fields (CSV only)
          Default : ","
        - quote_char : quote character (CSV only)
          Default : """""""
        - header :  skip header line (CSV only)
          Default : false
        - sheet : EXCEL sheet (EXCEL / ODF only)
          Default : 1st sheet
    """

    def __init__(self, fdw_options, fdw_columns):
        super(cloudfs_fdw, self).__init__(fdw_options, fdw_columns)
        self.source = fdw_options.get("source", 's3').lower()
        if self.source is None:
            log_to_postgres("Please set the source", ERROR)

        self.format = fdw_options.get("format", 'csv').lower()
        if self.format is None:
            log_to_postgres("Please set the file format", ERROR)

        self.region = fdw_options.get("region")
        # if self.region is None:
        #    log_to_postgres("Please set the AWS region", ERROR)

        self.host = fdw_options.get("host", "localhost")
        # if self.host is None:
        #    log_to_postgres("Please set the endpoint host", ERROR)

        self.port = int(fdw_options.get("port", "443"))
        # if self.port is None:
        #    log_to_postgres("Please set the endpoint port", ERROR)

        self.http_url = fdw_options.get("url")

        self.filepath = fdw_options.get("filepath")
        # if self.filename is None:
        #    log_to_postgres("Please set the filename", ERROR)

        self.bucket = fdw_options.get('bucket')
        # if self.bucket is None:
        #    log_to_postgres("Please set the bucket", ERROR)

        self.aws_access_key = fdw_options.get('aws_access_key')
        # if self.aws_access_key is None:
        #    log_to_postgres("Please set the AWS access key", ERROR)

        self.aws_secret_key = fdw_options.get('aws_secret_key')
        # if self.aws_secret_key is None:
        #    log_to_postgres("Please set the AWS secret key", ERROR)

        self.delimiter = fdw_options.get("delimiter", ",")

        self.quotechar = fdw_options.get("quote_char", '"')

        self.skip_header = ('true' == fdw_options.get(
            'header', 'false').lower())

        self.json_path = fdw_options.get('json_path', 'item')
        if 'item' != self.json_path:
            self.json_path = self.json_path + '.item'

        self.sheet = fdw_options.get('sheet', 0)

        self.columns = fdw_columns

    def can_sort(self, sortkeys):
        can_sort = []

        if self.format in ['xls', 'xlsx', 'odf']:
            return sortkeys
    
        return can_sort

    def execute(self, quals, columns, sortkeys=None):
        if 's3' == self.source:
            url = 's3://{}:{}@{}:{}@{}/{}'.format(
                self.aws_access_key, self.aws_secret_key, self.host, self.port, self.bucket, self.filepath)
        elif 'file' == self.source:
            url = 'file://{}'.format(self.filepath)
        elif 'http/https' == self.source:
            url = self.http_url
        else:
            log_to_postgres("Source {} not supported".format(self.source))

        data_stream = smart_open.open(url, 'rb')

        if 'csv' == self.format:
            for row in self._render_csv(data_stream):
                yield row

        elif 'json' == self.format:
            for row in self._render_json(data_stream):
                yield row

        elif self.format in ['xls', 'xlsx', 'odf']:
            for row in self._render_excel_or_odf(data_stream, sortkeys):
                yield row

        else:
            log_to_postgres("Format {} not supported".format(self.format))

    def _render_csv(self, data_stream):
        object_stream = csv.reader(data_stream, delimiter=self.delimiter,
                                   quotechar=self.quotechar)

        if (self.skip_header):
            for _ in object_stream:
                break

        for obj in object_stream:
            yield obj[:len(self.columns)]

    def _render_json(self, data_stream):
        object_stream = ijson.items(data_stream, self.json_path)

        for obj in object_stream:
            yield obj.values()[:len(self.columns)]

    def _render_excel_or_odf(self, data_stream, sortkeys):
        engine = 'xlrd'

        if self.format == 'odf':
            engine = 'odf'

        object_stream = pandas.read_excel(
            data_stream, sheet_name=self.sheet, header=0 if self.skip_header else None, engine=engine)

        if sortkeys:
            columns = object_stream.columns.values
            sort_columns = []
            sort_orders = []

            for sortkey in sortkeys:
                sort_columns.append(columns[sortkey.attnum - 1])
                sort_orders.append(not sortkey.is_reversed)

            object_stream.sort_values(by=sort_columns, axis=0, ascending=sort_orders, inplace=True)    

        for row in object_stream.iterrows():
            yield row[1].values[:len(self.columns)]

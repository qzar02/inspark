from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.group import GroupedData
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql import SparkSession

spark = (
        SparkSession
        .builder
        .getOrCreate())


methods = dir(DataFrame)


def functions_broadcast_mock(df):
    current_name = Refactoring.get_assign_name(df)
    assign_name = Refactoring.next_assign_name()

    Refactoring.new_line(assign_name, current_name, 'f.broadcast', '()')
    return spark.createDataFrame([], schema='a int').set_name(assign_name)


def format_params(args, kwargs={}):
    params = []

    for a in args:

        if isinstance(a, DataFrame):
            _arg = Refactoring.get_assign_name(a)
        elif isinstance(a, str):
            _arg = f"'{a}'"
        else:
            _arg = f"{a}"

        params.append(_arg)

    params.extend([f"{k}='{v}'" for k, v in kwargs.items()])

    return f'({", ".join(params)})'


class DataFrameMock:

    def __getitem__(self, name):
        if name not in self.columns:
            df = self.o_withColumn(name, f.lit(name.upper()))
            return df.o_getitem(name)
        else:
            return self.o_getitem(name)

    def __getattr__(self, name):
        if name not in self.columns:
            df = self.o_withColumn(name, f.lit(name.upper()))
            return df.o_getattr(name)
        else:
            return self.o_getattr(name)

    def set_name(self, name):
        self.assign_name = name
        return self


class GroupedDataMock:

    def agg(self, *exprs):
        current_name = Refactoring.get_assign_name(self)
        assign_name = Refactoring.next_assign_name()
        Refactoring.new_line(
            assign_name,
            current_name,
            'agg',
            format_params(exprs))
        return spark.createDataFrame([], schema='a int').set_name(assign_name)

    def count(self):
        current_name = Refactoring.get_assign_name(self)
        assign_name = Refactoring.next_assign_name()

        Refactoring.new_line(assign_name, current_name, 'count', '()')
        return spark.createDataFrame([], schema='a int').set_name(assign_name)


class DataFrameReaderMock:

    def load(self, *args, **kwargs):
        assign_name = Refactoring.next_assign_name()
        Refactoring.new_line(
            assign_name,
            None,
            'read.load',
            format_params(args, kwargs))
        return spark.createDataFrame([], schema='a int').set_name(assign_name)


class DataFrameWriterMock:

    def save(self, *args, **kwargs):
        current_name = Refactoring.get_assign_name(self)
        assign_name = Refactoring.next_assign_name()

        Refactoring.new_line(
            assign_name,
            current_name,
            'write.save',
            format_params(args, kwargs))


class Refactoring:

    # Contador de operações realizadas.
    # Esse contador é incrementado a cada chamada dos metodos mockados
    # e serve como parte do nome usado nas variaveis após cada transformação.
    counter_op = -1

    # output das operações
    output_text = ''
    output_nodes = []

    @staticmethod
    def next_assign_name():
        """
        Incrementa o contador de operações e retorna seu valor
        """
        Refactoring.counter_op += 1
        return f'df{Refactoring.counter_op}_'

    @staticmethod
    def new_line(assign_name, current_name, fn_name, params):
        sub_params = (
            params
            .replace('Column<b', '')
            .replace("'>", "'")
        )

        Refactoring.output_nodes.append(
            (assign_name, current_name or '', fn_name, sub_params)
        )

        Refactoring.output_text += (
            f'{assign_name} = {current_name}.{fn_name}{sub_params}\n'
            if current_name else
            f'{assign_name} = {fn_name}{sub_params}\n'
        )

    @staticmethod
    def get_assign_name(df):
        return df.assign_name

    def dataframe_mock(self, fn_name):

        def run(_self, *args, **kw):

            current_name = Refactoring.get_assign_name(_self)
            assign_name = Refactoring.next_assign_name()

            Refactoring.new_line(
                assign_name,
                current_name,
                fn_name,
                format_params(args, kw))

            if fn_name == 'save':
                return

            if fn_name == 'take':
                return _self.o_take(*args)

            if fn_name == 'collect':
                return _self.o_collect()

            if fn_name in ['groupby', 'groupBy']:
                df = None

                for col in args:
                    if isinstance(col, str):
                        df = (_self if df is None else df).o_withColumn(col, f.lit(col.upper()))

                grp = df.o_groupBy(*args, **kw)
                grp.assign_name = assign_name

                return grp

            new_df = (
                spark
                .createDataFrame([], schema='a int')
                .set_name(assign_name))

            return new_df

        return run

    def mock(self):

        for method_name in methods:

            if method_name.startswith('_'):
                continue

            df_method = getattr(DataFrame, method_name)

            if callable(df_method):
                setattr(DataFrame, 'o_' + method_name, df_method)
                setattr(
                    DataFrame,
                    method_name,
                    self.dataframe_mock(method_name))

        DataFrame.o_getitem = DataFrame.__getitem__
        DataFrame.__getitem__ = DataFrameMock.__getitem__

        DataFrame.o_getattr = DataFrame.__getattr__
        DataFrame.__getattr__ = DataFrameMock.__getattr__

        DataFrame.set_name = DataFrameMock.set_name

        GroupedData.o_agg = GroupedData.agg
        GroupedData.agg = GroupedDataMock.agg

        GroupedData.o_count = GroupedData.count
        GroupedData.count = GroupedDataMock.count

        DataFrameReader.o_load = DataFrameReader.load
        DataFrameReader.load = DataFrameReaderMock.load

        DataFrameWriter.o_save = DataFrameWriter.save
        DataFrameWriter.save = DataFrameWriterMock.save

        f.o_broadcast = f.broadcast
        f.broadcast = functions_broadcast_mock

from pyspark.sql.functions import lit, monotonically_increasing_id
import pandas as pd
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session


class NaverDatalabTransformer:

    @classmethod
    def transform(cls):
        movie_detail_df = find_data(DataWarehouse, 'MOVIE_DETAIL')
        movie_code_df = movie_detail_df.select('MOVIE_CODE')
        # movie_code_df.show()
        movie_code_list = movie_detail_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()

        # print(movie_code_list)

        for i in range(len(movie_code_list)):
            path = '/naver/datalab/naver_datalab_' + movie_code_list[i] + '.json'

            datalab = get_spark_session().read.json(path, encoding='UTF-8').first()

            results = get_spark_session().createDataFrame(datalab['results']).first()
            data = get_spark_session().createDataFrame(results['data'])
            # print(data)

            week = [j+1 for j in range(data.count())]
            # print(week)

            week_pandas_df = pd.DataFrame({'WEEK': week})
            # print(week_pandas_df)

            week_df = get_spark_session().createDataFrame(week_pandas_df)
            # week_df.show()

            week_df_idm = week_df.withColumn('idm', monotonically_increasing_id())
            # week_df_idm.show()
            # print(type(week_df_idm.weeks))  # <class 'pyspark.sql.column.Column'>
            
            datalab_df = data.withColumn('MOVIE_CODE', lit(movie_code_list[i])) \
                            .withColumn('idm', monotonically_increasing_id()) \
                            .withColumnRenamed('period', 'STD_DATE') \
                            .withColumnRenamed('ratio', 'RATIO')
            # datalab_df.show()

            merge_df = datalab_df.join(week_df_idm, on='idm', how='left')
            # merge_df.show()

            final_datalab_df = merge_df.drop(merge_df.idm)
            final_datalab_df.show()

            final_datalab_df.printSchema()

            save_data(DataWarehouse, final_datalab_df, 'MOVIE_SEARCH_RATIO')

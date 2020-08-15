from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category", output_feature_col="features", output_label_col="label"):
    
    WordTokenizer = Tokenizer(inputCol = input_descript_col,outputCol = "words")
    countVectors = CountVectorizer(inputCol = "words",outputCol = output_feature_col)
    label_String_index = StringIndexer(inputCol = input_category_col,outputCol = output_label_col)
    pipeline = Pipeline(stages = [WordTokenizer,countVectors,label_String_index])
    
    return pipeline

def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0, svm_1, svm_2):
    
    group_num = training_df.select("group").distinct().count()
    gen_base_pipeline = Pipeline(stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2])
    
    for i in range(group_num):
        tmp = prepare_data_model(training_df,gen_base_pipeline,i)
        if(i == 0):
            res = tmp
        else:
            res = res.union(tmp)
        
    res = combine_column(res)
    return res

def prepare_data_model(training_set,base_pipeline,group_index):
    
    gen_train_data = training_set.filter(training_set.group != group_index)
    gen_pre_data = training_set.filter(training_set.group == group_index)
    gen_pipeline_model = base_pipeline.fit(gen_train_data)
    res_df = gen_pipeline_model.transform(gen_pre_data)
    
    return res_df
    
def combine_column(dataframe):
    
    udf_function = udf(change_to_decimal,DoubleType())
    dataframe = dataframe.withColumn("joint_pred_0",udf_function("nb_pred_0","svm_pred_0"))
    dataframe = dataframe.withColumn("joint_pred_1",udf_function("nb_pred_1","svm_pred_1"))
    dataframe = dataframe.withColumn("joint_pred_2",udf_function("nb_pred_2","svm_pred_2"))
    
    return dataframe

def change_to_decimal(nb,svm):
    
    nb_int = int(nb)
    svm_int = int(svm)
    combine_str = str(nb_int) + str(svm_int)
    str_to_decimal = int(combine_str,2)
    change_to_float = float(str_to_decimal)
    
    return change_to_float
    
def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model, gen_meta_feature_pipeline_model, meta_classifier):
    
    Testdf = base_features_pipeline_model.transform(test_df)
    base_pred = gen_base_pred_pipeline_model.transform(Testdf)
    combine_df = combine_column(base_pred)
    gen_meta_features_pred = gen_meta_feature_pipeline_model.transform(combine_df)
    gen_meta_classifier = meta_classifier.transform(gen_meta_features_pred)
    res = gen_meta_classifier.select("id","label","final_prediction")
    
    return res
    
    

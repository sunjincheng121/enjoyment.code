


















# transformer
va = VectorAssembler()\
    .set_selected_cols(["sepal_length",
                        "sepal_width", "petal_length", "petal_width"])\
    .set_output_col("features")

# estimator
kmeans = KMeans() \
    .set_vector_col("features") \
    .set_k(3) \
    .set_reserved_cols(["sepal_length", "sepal_width",
                        "petal_length", "petal_width", "category"]) \
    .set_prediction_col("prediction_result")

# pipeline
pipeline = Pipeline().append_stage(va).append_stage(kmeans)
pipeline \
    .fit(t_env, sourceTable) \
    .transform(t_env, sourceTable) \
    .insert_into('kmeansResults')


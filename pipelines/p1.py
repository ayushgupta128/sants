from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "p1", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    p1__reformat_1 = Process(name = "p1__Reformat_1", properties = ModelTransform(modelName = "p1__Reformat_1"))
    p1__customer_master_scd = Process(
        name = "p1__customer_master_scd",
        properties = ModelTransform(modelName = "p1__customer_master_scd")
    )
    p1__reformat_1 >> p1__customer_master_scd
